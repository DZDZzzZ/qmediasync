package scrape

import (
	"Q115-STRM/internal/baidupan"
	"Q115-STRM/internal/db"
	"Q115-STRM/internal/helpers"
	"Q115-STRM/internal/models"
	"Q115-STRM/internal/openlist"
	"Q115-STRM/internal/tmdb"
	"Q115-STRM/internal/v115open"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

type tvShowScrapeImpl struct {
	ScrapeBase
	fileTasks    chan *tvshowTask
	episodeTasks chan uint
}

type tvshowTask struct {
	mediaFile *models.ScrapeMediaFile
	seasons   []uint
}

func NewTvShowScrapeImpl(scrapePath *models.ScrapePath, ctx context.Context, v115Client *v115open.OpenClient, openlistClient *openlist.Client, baiduPanClient *baidupan.Client) scrapeImpl {
	tmdbImpl := NewTmdbTvShowImpl(scrapePath, ctx)
	return &tvShowScrapeImpl{
		ScrapeBase: ScrapeBase{
			scrapePath:     scrapePath,
			ctx:            ctx,
			identifyImpl:   NewIdTvShowImpl(scrapePath, ctx, tmdbImpl),
			categoryImpl:   NewCategoryTvShowImpl(scrapePath),
			renameImpl:     NewRenameTvShowImpl(scrapePath, ctx, v115Client, openlistClient, baiduPanClient),
			tmdbClient:     tmdbImpl.Client,
			v115Client:     v115Client,
			baiduPanClient: baiduPanClient,
			openlistClient: openlistClient,
		},
	}
}

// 先处理电视剧：用PathId分组，每组取第一条，识别完后更新同步批次同PathId的所有记录的tmdbid，name, year，然后将这些ID放入待处理队列
func (t *tvShowScrapeImpl) Start() error {
	// 查询数据库中所有待刮削和待整理的记录总数来决定要启动的工作协程数量
	total := models.GetScannedScrapeMediaFilesTotal(t.scrapePath.ID, t.scrapePath.MediaType)
	if total == 0 {
		helpers.AppLogger.Infof("没有待刮削和待整理的记录，无需启动刮削任务")
		return nil
	}
	t.fileTasks = make(chan *tvshowTask, t.scrapePath.GetMaxThreads())
	t.episodeTasks = make(chan uint, 100)
	// 每次从数据库中查询maxthreads个任务加入队列，等待处理完成后继续下一次查询直到无法查询到数据
	// 启动N个协程协程，由m.ctx控制是否取消
	wg := &sync.WaitGroup{}
	episodeWg := &sync.WaitGroup{}
	for i := 0; i < t.scrapePath.GetMaxThreads(); i++ {
		go t.scrapeTvShow(i+1, wg, episodeWg)
		go t.scrapeEpisode(i+1, episodeWg)
	}
	// 启动一个协程检查是否停止
	stopChan := make(chan struct{})
	go func() {
		for {
			select {
			case <-t.ctx.Done():
			stoploop:
				for {
					select {
					case tvshowTask := <-t.fileTasks:
						helpers.AppLogger.Infof("清理剩余电视剧刮削任务 %d", tvshowTask.mediaFile.ID)
						wg.Done()
					case episodeMediaFileID := <-t.episodeTasks:
						helpers.AppLogger.Infof("清理剩余集刮削任务 %d", episodeMediaFileID)
						episodeWg.Done()
					default:
						break stoploop
					}
				}
				// helpers.AppLogger.Infof("所有待刮削和待整理记录都已处理")
				time.Sleep(time.Second)
				return
			case <-stopChan:
				helpers.AppLogger.Infof("刮削任务接收到停止信号，退出清理循环")
				return
			default:
				helpers.AppLogger.Infof("1秒后继续检查是否已经停止任务")
				time.Sleep(time.Second)
			}
		}
	}()
mainloop:
	for {
		select {
		case <-t.ctx.Done():
			helpers.AppLogger.Infof("刮削任务接收到取消信号，退出主循环")
			break mainloop
		default:
		}
		// 从数据库取数据
		// 扫描阶段已经提取了季和集的序号，这里获取到的是用剧分组的所有数据，需要处理成剧->季的形式
		mediaFiles := models.GetScannedScrapeMediaFilesGroupByTvshowPathId(t.scrapePath.ID, t.scrapePath.GetMaxThreads()*2)
		if len(mediaFiles) == 0 {
			helpers.AppLogger.Infof("所有待刮削和待整理记录都已加入处理队列，关闭队列通道，等待执行完成")
			break mainloop
		}
		tvshowTasks := make(map[string]*tvshowTask, 0)
	fileloop:
		for _, mediaFile := range mediaFiles {
			// helpers.AppLogger.Infof("处理电视剧 %s 季 %d", filepath.Base(mediaFile.TvshowPath), mediaFile.SeasonNumber)
			if _, ok := tvshowTasks[mediaFile.TvshowPathId]; !ok {
				tvshowTasks[mediaFile.TvshowPathId] = &tvshowTask{
					mediaFile: mediaFile,
					seasons:   make([]uint, 0),
				}
			}
			// 去重
			if slices.Contains(tvshowTasks[mediaFile.TvshowPathId].seasons, mediaFile.ID) {
				continue fileloop
			}
			tvshowTasks[mediaFile.TvshowPathId].seasons = append(tvshowTasks[mediaFile.TvshowPathId].seasons, mediaFile.ID)
		}
		for _, tvshowTask := range tvshowTasks {
			t.fileTasks <- tvshowTask
			wg.Add(1) // 加进去之后，计数+1
			helpers.AppLogger.Infof("电视剧 %s 已加入处理队列", tvshowTask.mediaFile.VideoFilename)
		}
		helpers.AppLogger.Infof("已加入 %d 个季到处理队列，等待刮削完成", len(tvshowTasks))
		wg.Wait()
		episodeWg.Wait() // 等待集处理队列完成
	}
	helpers.AppLogger.Infof("所有集刮削整理任务都已完成，发送停止信号")
	select {
	case stopChan <- struct{}{}:
		helpers.AppLogger.Infof("已发送信号让所有清理监控任务退出")
	default:
	}
	close(stopChan)
	close(t.fileTasks)
	close(t.episodeTasks)
	helpers.AppLogger.Infof("所有刮削整理任务都已完成，本次任务结束")
	return nil
}

func (t *tvShowScrapeImpl) scrapeTvShow(taskIndex int, wg *sync.WaitGroup, episodeWg *sync.WaitGroup) {
mainloop:
	for {
		select {
		case <-t.ctx.Done():
			helpers.AppLogger.Infof("电视剧工作线程 %d 检测到停止信号，退出", taskIndex)
			return
		case tt, ok := <-t.fileTasks:
			if !ok {
				helpers.AppLogger.Infof("电视剧工作线程 %d 已关闭", taskIndex)
				return
			}
			helpers.AppLogger.Infof("电视剧工作线程 %d 开始处理电视剧 %s", taskIndex, filepath.Base(tt.mediaFile.TvshowPath))
			err := t.ProcessTvShow(tt)
			if err != nil {
				wg.Done()
				helpers.AppLogger.Errorf("电视剧工作线程 %d 刮削电视剧 %s 失败: %v", taskIndex, filepath.Base(tt.mediaFile.TvshowPath), err)
				continue mainloop
			}
			helpers.AppLogger.Infof("电视剧工作线程 %d 完成处理电视剧 %s", taskIndex, filepath.Base(tt.mediaFile.TvshowPath))
		seasonloop:
			for _, seasonMediaFileID := range tt.seasons {
				if !t.scrapePath.IsRunning() {
					helpers.AppLogger.Infof("电视剧工作线程 %d 检测到刮削任务已停止，退出", taskIndex)
					break seasonloop
				}
				seasonMediaFile := models.GetScrapeMediaFileById(seasonMediaFileID)
				helpers.AppLogger.Infof("电视剧工作线程 %d 开始处理电视剧 %s 季 %d", taskIndex, filepath.Base(tt.mediaFile.TvshowPath), seasonMediaFile.SeasonNumber)
				serr := t.ProcessSeason(seasonMediaFile)
				if serr != nil {
					helpers.AppLogger.Errorf("电视剧工作线程 %d 刮削电视剧 %s 季 %d 失败: %v", taskIndex, filepath.Base(tt.mediaFile.TvshowPath), seasonMediaFile.SeasonNumber, serr)
					continue seasonloop
				}
				helpers.AppLogger.Infof("电视剧工作线程 %d 完成处理电视剧 %s 季 %d", taskIndex, filepath.Base(tt.mediaFile.TvshowPath), seasonMediaFile.SeasonNumber)
				// 将季下所有集加入处理队列
				episodeMediaFileIds := models.GetScrapeMediaFileIdBySeasonId(seasonMediaFile.MediaSeasonId)
				if len(episodeMediaFileIds) == 0 {
					helpers.AppLogger.Infof("电视剧工作线程 %d 季 %d 下没有集，跳过", taskIndex, seasonMediaFile.SeasonNumber)
					continue seasonloop
				}
				for _, episodeMediaFileID := range episodeMediaFileIds {
					if !t.scrapePath.IsRunning() {
						helpers.AppLogger.Infof("电视剧工作线程 %d 检测到刮削任务已停止，退出", taskIndex)
						break seasonloop
					}
					t.episodeTasks <- episodeMediaFileID
					episodeWg.Add(1) // 加进去之后，计数+1
					helpers.AppLogger.Infof("电视剧工作线程 %d 已将 %d 加入集处理队列", taskIndex, episodeMediaFileID)
				}
			}
			// 处理完成后，计数-1
			wg.Done()
		}
	}
}

// 根据提取的信息，先确定电视剧和季
// 然后查询电视剧和季是否存在，如果存在未刮削则刮削电视剧和季；如果存在已刮削，则刮削集
// 整理阶段，先查询电视剧和季是否整理，如果未整理先整理电视剧和季，然后整理集
func (t *tvShowScrapeImpl) ProcessTvShow(tt *tvshowTask) error {
	mediaFile := tt.mediaFile
	// 创建临时目录
	mediaFile.ScrapeRootPath = filepath.Join(helpers.ConfigDir, "tmp", "刮削临时文件", fmt.Sprintf("%d", mediaFile.ScrapePathId), "电视剧")
	if err := os.MkdirAll(mediaFile.ScrapeRootPath, 0777); err != nil {
		helpers.AppLogger.Errorf("创建临时目录失败: %v", err)
		return err
	}
	// 先从文件名或文件夹名字中提取影片名字+年份或tmdbid
	if mediaFile.Media == nil || (mediaFile.Media != nil && mediaFile.Media.Status == models.MediaStatusUnScraped) {
		// 检查路径，是tvshow/season/episode.mkv 还是tvshow/episode.mkv
		t.FillTvshowPath(mediaFile)
		// 待刮削，启动刮削流程
		err := t.ScrapeTvshow(mediaFile)
		if err != nil {
			// 将电视剧下所有集标记为失败
			t.ScrapeFailedAllEdpisode(mediaFile, err.Error())
			return err
		}
		// 更新电视剧下的所有集的数据
		t.UpdateTvshowDataToAllEpisode(mediaFile)
	}
	if mediaFile.Media != nil && mediaFile.Media.Status == models.MediaStatusScraped {
		// 如果已刮削则整理
		// 整理电视剧
		// 在目标位置创建新目录
		// 上传电视剧的元数据
		// 修改季下所有集的数据
		// 将所有集加入处理队列
		t.MakeTvshowPath(mediaFile, t.scrapePath.CategoryMap)
		// 上传所有刮削好的电视剧的元数据
		if t.scrapePath.ScrapeType != models.ScrapeTypeOnlyRename {
			if uerr := t.UploadTvshowScrapeFile(mediaFile); uerr != nil {
				// 将电视剧下所有集标记为失败
				t.RenamedFailedAllEdpisode(mediaFile, uerr.Error())
				return uerr
			}
			// 将电视剧标记为已整理
			mediaFile.Media.Status = models.MediaStatusRenamed
			mediaFile.Media.Save()
			helpers.AppLogger.Infof("电视剧 %s 元数据上传完成，标记为已整理", mediaFile.Media.Name)
		}
	}
	return nil
}

func (t *tvShowScrapeImpl) FillTvshowPath(mediaFile *models.ScrapeMediaFile) error {
	// 根据tvshowPath确定tvshowPathId
	if t.scrapePath.SourceType == models.SourceType115 && mediaFile.TvshowPathId == "" && mediaFile.TvshowPath != "" {
		// 从115中查询tvshowPathId
		tvshowPathDetail, err := t.v115Client.GetFsDetailByPath(t.ctx, mediaFile.TvshowPath)
		if err != nil {
			helpers.AppLogger.Errorf("从115中查询电视剧目录详情 %s 失败: %v", mediaFile.TvshowPath, err)
			return err
		}
		mediaFile.TvshowPathId = tvshowPathDetail.FileId
	}
	return nil
}

func (t *tvShowScrapeImpl) ScrapeTvshow(mediaFile *models.ScrapeMediaFile) error {
	// 识别
	if err := t.identifyImpl.Identify(mediaFile); err != nil {
		return err
	}
	// 刮削电视剧
	if scrapeErr := t.ScrapeTvshowMedia(mediaFile); scrapeErr != nil {
		return scrapeErr
	}
	// 确定二级分类
	if cerr := t.GenrateCategory(mediaFile); cerr != nil {
		return cerr
	}
	t.GenerateNewTvshowName(mediaFile)
	if mediaFile.ScrapeType != models.ScrapeTypeOnlyRename {
		// 下载图片，生成nfo文件
		// 生成本地临时路径
		localTempPath := mediaFile.GetTmpFullTvshowPath()
		if err := os.MkdirAll(localTempPath, 0777); err != nil {
			helpers.AppLogger.Errorf("创建临时目录 %s 失败，下次重试，错误: %v", localTempPath, err)
			return err
		} else {
			helpers.AppLogger.Infof("临时目录 %s 创建成功", localTempPath)
		}
		// 生成nfo
		t.GenerateTvShowNfo(mediaFile, localTempPath, t.scrapePath.ExcludeNoImageActor)
		fileList := map[string]string{}
		fileList[t.GetTvshowRealName(mediaFile, "poster.jpg", "image")] = mediaFile.Media.PosterPath
		fileList[t.GetTvshowRealName(mediaFile, "clearlogo.jpg", "image")] = mediaFile.Media.LogoPath
		fileList[t.GetTvshowRealName(mediaFile, "fanart.jpg", "image")] = mediaFile.Media.BackdropPath
		t.DownloadImages(localTempPath, v115open.DEFAULTUA, fileList)
	}
	return nil
}

func (t *tvShowScrapeImpl) ScrapeTvshowMedia(mediaFile *models.ScrapeMediaFile) error {
	helpers.AppLogger.Infof("刮削电视剧, 名字=%s，年份=%d, tmdbid=%d", mediaFile.Name, mediaFile.Year, mediaFile.TmdbId)
	tmdbInfo := &models.TmdbInfo{}
	// 查询详情
	tvDetail, err := t.tmdbClient.GetTvDetail(mediaFile.TmdbId, models.GlobalScrapeSettings.GetTmdbLanguage())
	if err != nil {
		helpers.AppLogger.Errorf("查询tmdb电视剧详情失败, 下次重试, 失败原因: %v", err)
		return err
	}
	tmdbInfo.TvShowDetail = tvDetail
	// 查询演职人员
	cast, _ := t.tmdbClient.GetTvCredits(mediaFile.TmdbId, models.GlobalScrapeSettings.GetTmdbLanguage())
	tmdbInfo.Credits = cast
	// 查询图片
	images, _ := t.tmdbClient.GetTvImages(mediaFile.TmdbId, models.GlobalScrapeSettings.GetTmdbImageLanguage())
	if images != nil {
		// 如果图片为空,则使用详情中的图片
		if len(images.Posters) == 0 && tvDetail.PosterPath != "" {
			images.Posters = append(images.Posters, tmdb.Image{
				FilePath: tvDetail.PosterPath,
			})
		}
		if len(images.Backdrops) == 0 && tvDetail.BackdropPath != "" {
			images.Backdrops = append(images.Backdrops, tmdb.Image{
				FilePath: tvDetail.BackdropPath,
			})
		}
		tmdbInfo.Images = images
	} else {
		tmdbInfo.Images = &tmdb.Images{}
		if tvDetail.PosterPath != "" {
			tmdbInfo.Images.Posters = append(tmdbInfo.Images.Posters, tmdb.Image{
				FilePath: tvDetail.PosterPath,
			})
		}
		if tvDetail.BackdropPath != "" {
			tmdbInfo.Images.Backdrops = append(tmdbInfo.Images.Backdrops, tmdb.Image{
				FilePath: tvDetail.BackdropPath,
			})
		}
	}
	// 使用tmdbinfo补全media的信息
	t.MakeMediaFromTMDB(mediaFile, tmdbInfo)
	return nil
}

func (t *tvShowScrapeImpl) GetTvshowUploadFiles(mediaFile *models.ScrapeMediaFile) []uploadFile {
	destPath := mediaFile.GetDestFullTvshowPath()
	destPathId := mediaFile.NewPathId
	tvshowSourcePath := mediaFile.GetTmpFullTvshowPath()
	fileList := make([]uploadFile, 0)
	nfoName := t.GetTvshowRealName(mediaFile, "", "nfo")
	nfoPath := filepath.Join(tvshowSourcePath, nfoName)
	helpers.AppLogger.Infof("nfo文件路径 %s", nfoPath)
	if helpers.PathExists(nfoPath) {
		file := uploadFile{
			ID:         fmt.Sprintf("%d", mediaFile.ID),
			FileName:   nfoName,
			SourcePath: nfoPath,
			DestPath:   destPath,
			DestPathId: destPathId,
		}

		fileList = append(fileList, file)
	}
	imageList := []string{"poster.jpg", "clearlogo.jpg", "clearart.jpg", "square.jpg", "logo.jpg", "fanart.jpg", "backdrop.jpg", "background.jpg", "4kbackground.jpg", "thumb.jpg", "banner.jpg", "disc.jpg"}
	for _, im := range imageList {
		name := t.GetTvshowRealName(mediaFile, im, "image")
		sPath := filepath.Join(tvshowSourcePath, name)
		// helpers.AppLogger.Infof("图片文件路径 %s", sPath)
		if helpers.PathExists(sPath) {
			file := uploadFile{
				ID:         fmt.Sprintf("%d", mediaFile.ID),
				FileName:   name,
				SourcePath: sPath,
				DestPath:   destPath,
				DestPathId: destPathId,
			}
			fileList = append(fileList, file)
		}
	}
	return fileList
}

// 上传电视剧所有生成好的文件
// 先上传电视剧和季的
// 再上传集的
func (t *tvShowScrapeImpl) UploadTvshowScrapeFile(mediaFile *models.ScrapeMediaFile) error {
	helpers.AppLogger.Infof("开始处理电视剧 %s 的元数据上传", mediaFile.Name)
	files := t.GetTvshowUploadFiles(mediaFile)
	// 如果是本地文件直接移动到目标位置
	ok, err := t.MoveLocalTempFileToDest(mediaFile, files)
	if err == nil {
		helpers.AppLogger.Infof("移动本地临时文件到目标位置成功")
		return nil
	}
	if !ok {
		helpers.AppLogger.Errorf("移动本地临时文件到目标位置失败, 失败原因: %v", err)
		// 标记为失败
		return err
	}
	// 将上传文件添加到上传队列
	for _, file := range files {
		if !helpers.PathExists(file.SourcePath) {
			helpers.AppLogger.Errorf("本地临时文件 %s 不存在，跳过上传", file.SourcePath)
			continue
		}
		err := models.AddUploadTaskFromMediaFile(mediaFile, t.scrapePath, file.FileName, file.SourcePath, filepath.Join(file.DestPath, file.FileName), file.DestPathId, true)
		if err != nil {
			helpers.AppLogger.Errorf("添加上传任务 %s 失败, 失败原因: %v", file.FileName, err)
		}
	}
	return nil
}

func (t *tvShowScrapeImpl) MakeMediaFromTMDB(mediaFile *models.ScrapeMediaFile, tmdbInfo *models.TmdbInfo) {
	if mediaFile.MediaId != 0 {
		mediaFile.QueryRelation()
	}
	if mediaFile.Media == nil {
		mediaFile.Media = &models.Media{
			ScrapePathId: mediaFile.ScrapePathId,
			MediaType:    mediaFile.MediaType,
			Name:         mediaFile.Name,
			Year:         mediaFile.Year,
			TmdbId:       mediaFile.TmdbId,
			Status:       models.MediaStatusUnScraped,
		}
		helpers.AppLogger.Infof("创建新的Media对象: %s, TMDBID=%d, 类型=%s", mediaFile.Media.Name, mediaFile.Media.TmdbId, mediaFile.Media.MediaType)
	}
	mediaFile.Media.FillInfoByTmdbInfo(tmdbInfo)
	mediaFile.MediaId = mediaFile.Media.ID
	mediaFile.Name = mediaFile.Media.Name
	mediaFile.Year = mediaFile.Media.Year
	mediaFile.Save()
}

func (t *tvShowScrapeImpl) GenrateCategory(mediaFile *models.ScrapeMediaFile) error {
	// 处理二级分类
	if !mediaFile.EnableCategory {
		return nil
	}
	categoryName, scrapePathCategory := t.categoryImpl.DoCategory(mediaFile)
	if categoryName == "" && scrapePathCategory == nil {
		// 无法确定二级分类则停止刮削
		helpers.AppLogger.Errorf("根据流派ID和语言确定电视剧的二级分类失败, 文件名: %s", mediaFile.Name)
		mediaFile.Failed("根据流派ID和语言确定电视剧的二级分类失败，停止刮削")
		return errors.New("根据流派ID和语言确定电视剧的二级分类失败")
	}
	mediaFile.CategoryName = categoryName
	mediaFile.ScrapePathCategoryId = scrapePathCategory.ID
	// 保存
	mediaFile.Save()
	helpers.AppLogger.Infof("根据流派ID和语言确定二级分类: %s, 分类目录ID:%s", categoryName, scrapePathCategory.FileId)
	return nil
}

// 生成新的电视剧路径
func (t *tvShowScrapeImpl) GenerateNewTvshowName(mediaFile *models.ScrapeMediaFile) {
	remotePath := mediaFile.GetRemoteTvshowPath() // 不含SourcePath的路径
	oldPathName := filepath.Base(remotePath)
	if mediaFile.ScrapeType == models.ScrapeTypeOnly {
		mediaFile.NewPathName = oldPathName
		return
	}
	folderTemplate := t.scrapePath.FolderNameTemplate
	if folderTemplate == "" && remotePath == "" {
		folderTemplate = "{title} ({year})"
	}
	// 根据命名规则生成文件夹名称
	if t.scrapePath.FolderNameTemplate == "" {
		if remotePath == "" {
			mediaFile.NewPathName = mediaFile.GenerateNameByTemplate(folderTemplate)
		} else {
			mediaFile.NewPathName = oldPathName
		}
	} else {
		mediaFile.NewPathName = mediaFile.GenerateNameByTemplate(t.scrapePath.FolderNameTemplate)
	}
	mediaFile.Media.Path = filepath.Join(mediaFile.DestPath, mediaFile.CategoryName, mediaFile.NewPathName)
	// 保存
	mediaFile.Save()
	mediaFile.Media.Save()
}

func (t *tvShowScrapeImpl) GetTvshowRealName(mediaFile *models.ScrapeMediaFile, name string, filetype string) string {
	if filetype == "nfo" {
		return "tvshow.nfo"
	}
	return name
}

func (t *tvShowScrapeImpl) GenerateTvShowNfo(mediaFile *models.ScrapeMediaFile, localTempPath string, excludeNoImageActor bool) error {
	// 解析tmdb genre
	nfoPath := filepath.Join(localTempPath, "tvshow.nfo")
	rates := []helpers.Rating{
		{
			Name:  "tmdb",
			Max:   10,
			Value: mediaFile.Media.VoteAverage,
			Votes: mediaFile.Media.VoteCount,
		},
	}
	genres := make([]string, 0)
	for _, genre := range mediaFile.Media.Genres {
		genres = append(genres, genre.Name)
	}
	has, result := helpers.ChineseToPinyin(mediaFile.Media.Name)
	originalTitle := mediaFile.Media.OriginalName
	SortTitle := mediaFile.Media.Name
	if has {
		originalTitle = fmt.Sprintf("%s #(%s)", mediaFile.Media.Name, result)
		SortTitle = fmt.Sprintf("%s #(%s)", result, mediaFile.Media.Name)
	}
	tv := &helpers.TVShow{
		Title:         mediaFile.Media.Name,
		OriginalTitle: originalTitle,
		SortTitle:     SortTitle,
		Ratings: struct {
			Rating []helpers.Rating `xml:"rating,omitempty"`
		}{
			Rating: rates,
		},
		UserRating: mediaFile.Media.VoteAverage,
		Outline:    fmt.Sprintf("<![CDATA[%s]]>", mediaFile.Media.Overview),
		Plot:       fmt.Sprintf("<![CDATA[%s]]>", mediaFile.Media.Overview),
		Tagline:    mediaFile.Media.Tagline,
		Year:       mediaFile.Media.Year,
		DateAdded:  time.Now().Format("2006-01-02"),
		Genre:      genres,
		// Actor:      mediaFile.Media.Actors,
		Director:  mediaFile.Media.Director,
		Id:        mediaFile.Media.ImdbId,
		TmdbId:    mediaFile.Media.TmdbId,
		ImdbId:    mediaFile.Media.ImdbId,
		Premiered: mediaFile.Media.ReleaseDate,
		Aired:     mediaFile.Media.ReleaseDate,
		Uniqueid: []helpers.UniqueId{
			{
				Id:      mediaFile.Media.ImdbId,
				Type:    "imdb",
				Default: true,
			},
			{
				Id:      fmt.Sprintf("%d", mediaFile.TmdbId),
				Type:    "tmdb",
				Default: false,
			},
		},
	}
	if excludeNoImageActor {
		tv.Actor = make([]helpers.Actor, 0)
		for _, actor := range mediaFile.Media.Actors {
			if actor.Thumb != "" {
				tv.Actor = append(tv.Actor, actor)
			}
		}
	} else {
		tv.Actor = mediaFile.Media.Actors
	}
	err := helpers.WriteTVShowNfo(tv, nfoPath)
	if err != nil {
		helpers.AppLogger.Errorf("生成电视剧nfo文件失败，文件路径：%s 错误： %v", nfoPath, err)
		return err
	} else {
		helpers.AppLogger.Infof("生成电视剧nfo文件成功，文件路径：%s", nfoPath)
	}
	return nil
}

// 创建父文件夹，电影是电影目录
func (t *tvShowScrapeImpl) MakeTvshowPath(mediaFile *models.ScrapeMediaFile, categoryMap map[uint]string) error {
	newPathId := ""
	if mediaFile.ScrapeType == models.ScrapeTypeOnly {
		newPathId = mediaFile.TvshowPathId
		// mediaFile.Media.PathId = mediaFile.TvshowPathId
		// mediaFile.Save()
		// mediaFile.Media.Save()
		// helpers.AppLogger.Infof("仅刮削模式下，使用旧目录存放元数据：%s，目录ID：%s", mediaFile.Path, mediaFile.TvshowPathId)
		// return nil
	} else {
		parentId := mediaFile.DestPathId
		if mediaFile.ScrapePathCategoryId > 0 {
			if category, ok := categoryMap[mediaFile.ScrapePathCategoryId]; ok {
				parentId = category
			}
		}
		destFullPath := mediaFile.GetDestFullTvshowPath()
		helpers.AppLogger.Infof("电视剧文件夹，目标路径：%s，根目录ID：%s", destFullPath, parentId)
		var err error
		newPathId, err = t.renameImpl.CheckAndMkDir(destFullPath, mediaFile.DestPath, mediaFile.DestPathId)
		if err != nil {
			helpers.AppLogger.Errorf("创建电视剧文件夹失败: %v", err)
			return err
		}
	}
	mediaFile.NewPathId = newPathId
	mediaFile.Media.PathId = newPathId
	mediaFile.Save()
	mediaFile.Media.Save()
	// 更新电视剧下所有集的NewPathId
	t.UpdateNewPathIdToAllEpisode(mediaFile)
	return nil
}

func (t *tvShowScrapeImpl) UpdateTvshowDataToAllEpisode(mediaFile *models.ScrapeMediaFile) error {
	// 更新电视剧下的所有集的数据
	// 如果是电视剧，则更新所有同剧的文件的NewPathId为新创建的文件夹ID
	// 批量更新
	// 将所有其他相同电视剧的季也修改信息
	// 构造更新数据
	updateData := map[string]interface{}{
		"media_id":                mediaFile.MediaId,
		"tvshow_path_id":          mediaFile.TvshowPathId,
		"name":                    mediaFile.Name,
		"year":                    mediaFile.Year,
		"tmdb_id":                 mediaFile.TmdbId,
		"new_path_name":           mediaFile.NewPathName,
		"new_path_id":             mediaFile.NewPathId,
		"category_name":           mediaFile.CategoryName,
		"scrape_path_category_id": mediaFile.ScrapePathCategoryId,
	}
	// 批量更新
	affectedRows := db.Db.Table("scrape_media_files").Where("scrape_path_id =? AND tvshow_path = ? AND batch_no = ?", mediaFile.ScrapePathId, mediaFile.TvshowPath, mediaFile.BatchNo).Updates(updateData).RowsAffected
	if affectedRows == 0 {
		helpers.AppLogger.Errorf("批量更新电视剧 %s 的所有集的信息失败, 未更新任何行", mediaFile.Name)
		return errors.New("no rows affected")
	}
	helpers.AppLogger.Infof("批量更新电视剧 %s 的所有集的信息成功，共更新 %d 行", mediaFile.Name, affectedRows)
	return nil
}

// 更新电视剧下所有集的NewPathId
func (t *tvShowScrapeImpl) UpdateNewPathIdToAllEpisode(mediaFile *models.ScrapeMediaFile) error {
	// 更新所有集的NewPathId为新创建的文件夹ID
	affectedRows := db.Db.Table("scrape_media_files").Where("scrape_path_id =? AND tvshow_path_id = ? AND batch_no = ?", mediaFile.ScrapePathId, mediaFile.TvshowPathId, mediaFile.BatchNo).Updates(map[string]interface{}{
		"new_path_id": mediaFile.NewPathId,
	}).RowsAffected
	if affectedRows == 0 {
		helpers.AppLogger.Errorf("批量更新电视剧 %s 的所有集的新目录ID失败, 未更新任何行", mediaFile.Name)
		return errors.New("no rows affected")
	}
	helpers.AppLogger.Infof("批量更新电视剧 %s 的所有集的新目录ID成功，共更新 %d 行", mediaFile.Name, affectedRows)
	return nil
}

func (t *tvShowScrapeImpl) ScrapeFailedAllEdpisode(mediaFile *models.ScrapeMediaFile, failedReason string) error {
	// 将所有集标记为失败
	err := db.Db.Table("scrape_media_files").Where("tvshow_path = ? AND batch_no = ?", mediaFile.TvshowPathId, mediaFile.BatchNo).Updates(map[string]interface{}{
		"status":        models.ScrapeMediaStatusScrapeFailed,
		"failed_reason": failedReason,
	}).Error
	if err != nil {
		helpers.AppLogger.Errorf("批量更新电视剧 %s 的所有集为刮削失败状态失败, 失败原因: %v", mediaFile.Name, err)
		return err
	}
	helpers.AppLogger.Infof("批量更新电视剧 %s 的所有集为刮削失败状态成功", mediaFile.Name)
	return nil
}

func (t *tvShowScrapeImpl) RenamedFailedAllEdpisode(mediaFile *models.ScrapeMediaFile, failedReason string) error {
	// 将所有集标记为失败
	err := db.Db.Table("scrape_media_files").Where("tvshow_path = ? AND batch_no = ?", mediaFile.TvshowPathId, mediaFile.BatchNo).Updates(map[string]interface{}{
		"status":        models.ScrapeMediaStatusRenameFailed,
		"failed_reason": failedReason,
	}).Error
	if err != nil {
		helpers.AppLogger.Errorf("批量更新电视剧 %s 的所有集为整理失败状态失败, 失败原因: %v", mediaFile.Name, err)
		return err
	}
	helpers.AppLogger.Infof("批量更新电视剧 %s 的所有集为整理失败状态成功", mediaFile.Name)
	return nil
}

// 检查剧和季是否已回滚，没有的话先回滚剧和季
// 回滚集
// 注意：上游此函数曾是 `return nil` 的空壳（整个实现被注释掉），导致回滚定时任务
// 每10 秒重复捞起同一条记录、打印"成功回滚"却什么都不做，形成死循环。
// 下面恢复对三个真实实现的调用；缺失的 Media/MediaSeason/MediaEpisode 关系按"跳过该层"处理，
// 而不是直接报错，避免个别集元数据不全时整条回滚卡死。
func (t *tvShowScrapeImpl) Rollback(mediaFile *models.ScrapeMediaFile) error {
	mediaFile.QueryRelation()

	if mediaFile.Media != nil && mediaFile.Media.Status != models.MediaStatusUnScraped {
		if err := t.RollbackTvShow(mediaFile); err != nil {
			helpers.AppLogger.Errorf("回滚电视剧 %s 失败, 失败原因: %v", mediaFile.Name, err)
			return err
		}
		helpers.AppLogger.Infof("回滚电视剧 %s 成功", mediaFile.Name)
	}

	if mediaFile.MediaSeason != nil && mediaFile.MediaSeason.Status != models.MediaStatusUnScraped {
		if err := t.RollbackTvShowSeason(mediaFile); err != nil {
			helpers.AppLogger.Errorf("回滚电视剧 %s 季 %d 失败, 失败原因: %v", mediaFile.Name, mediaFile.SeasonNumber, err)
			return err
		}
		helpers.AppLogger.Infof("回滚电视剧 %s 季 %d 成功", mediaFile.Name, mediaFile.SeasonNumber)
	}

	if mediaFile.MediaEpisode == nil {
		// 没有集级元数据可回滚，但剧/季可能已处理完，仍然算成功，
		// 否则这条记录会永远停在 rollbacking 被定时任务反复捞取
		helpers.AppLogger.Warnf("电视剧 %s 集 %d 缺少集级元数据，跳过集回滚", mediaFile.Name, mediaFile.EpisodeNumber)
	} else if err := t.RollbackEpisode(mediaFile); err != nil {
		helpers.AppLogger.Errorf("回滚电视剧 %s 季 %d 集 %d 失败, 失败原因: %v", mediaFile.Name, mediaFile.SeasonNumber, mediaFile.EpisodeNumber, err)
		return err
	}

	// 整批回滚完成后收敛：把同批记录重置回待刮削，刮削任务会自动按新识别信息重新整理。
	// 没有这一步的话记录会永远停在 rollbacking，被回滚定时任务反复捞取，
	// 30 分钟后被兜底误判为"回滚超时失败"。
	t.finishBatchRollback(mediaFile)
	return nil
}

// 检查同批次（同剧目录 + batch_no）是否还有回滚中的记录；
// 当前记录是最后一条时，把整批记录重置为待刮削（scanned），并删除旧的 Media/MediaSeason/MediaEpisode 行。
// 保留 ReScrape 写入的新识别信息（name/year/tmdb_id/季集编号）和 is_re_scrape/re_scrape_time，
// 下次刮削任务运行时 Identify 会直接使用新的 TMDB ID 重新刮削、重新整理。
func (t *tvShowScrapeImpl) finishBatchRollback(mediaFile *models.ScrapeMediaFile) {
	// 批次条件与 ReScrape 保持一致：优先 tvshow_path_id，115 无剧目录 ID 时退回 path_id
	batchCond := "tvshow_path_id = ? AND batch_no = ?"
	batchArg := any(mediaFile.TvshowPathId)
	if mediaFile.TvshowPathId == "" {
		batchCond = "path_id = ? AND batch_no = ?"
		batchArg = any(mediaFile.PathId)
	}
	var remaining int64
	if err := db.Db.Model(&models.ScrapeMediaFile{}).
		Where(batchCond+" AND status = ? AND id != ?", batchArg, mediaFile.BatchNo, models.ScrapeMediaStatusRollbacking, mediaFile.ID).
		Count(&remaining).Error; err != nil {
		helpers.AppLogger.Errorf("查询同批次剩余回滚中记录失败: %v", err)
		return
	}
	if remaining > 0 {
		// 同批还有记录没回滚完，由最后一条触发收敛
		return
	}

	// 收集整批记录引用的旧元数据行 ID
	var batchFiles []*models.ScrapeMediaFile
	if err := db.Db.Where(batchCond+" AND status = ?", batchArg, mediaFile.BatchNo, models.ScrapeMediaStatusRollbacking).Find(&batchFiles).Error; err != nil {
		helpers.AppLogger.Errorf("查询同批次回滚中的记录失败: %v", err)
		return
	}
	mediaIds := make(map[uint]bool)
	seasonIds := make(map[uint]bool)
	episodeIds := make(map[uint]bool)
	for _, f := range batchFiles {
		if f.MediaId != 0 {
			mediaIds[f.MediaId] = true
		}
		if f.MediaSeasonId != 0 {
			seasonIds[f.MediaSeasonId] = true
		}
		if f.MediaEpisodeId != 0 {
			episodeIds[f.MediaEpisodeId] = true
		}
	}

	// 清理按旧识别结果整理出的目标目录（此时集文件已搬回，目录里只剩剧/季级元数据）。
	// 必须在重置派生字段之前调用：清理依赖 batchFiles 里仍保留的 NewPathId/NewSeasonPathName 等旧值
	t.cleanupOldTargetDirs(batchFiles)

	// 重置为待刮削，清空按旧识别结果计算的派生字段（与 ReScrape 的重置分支一致）
	updateData := map[string]interface{}{
		"status":                  models.ScrapeMediaStatusScanned,
		"media_id":                0,
		"media_season_id":         0,
		"media_episode_id":        0,
		"failed_reason":           "",
		"category_name":           "",
		"scrape_path_category_id": 0,
		"new_path_name":           "",
		"new_season_path_name":    "",
		"new_path_id":             "",
		"new_season_path_id":      "",
		"new_video_base_name":     "",
	}
	res := db.Db.Model(&models.ScrapeMediaFile{}).
		Where(batchCond+" AND status = ?", batchArg, mediaFile.BatchNo, models.ScrapeMediaStatusRollbacking).
		Updates(updateData)
	if res.Error != nil {
		helpers.AppLogger.Errorf("回滚完成后重置批次记录为待刮削失败: %v", res.Error)
		return
	}
	for id := range mediaIds {
		db.Db.Delete(&models.Media{}, id)
	}
	for id := range seasonIds {
		db.Db.Delete(&models.MediaSeason{}, id)
	}
	for id := range episodeIds {
		db.Db.Delete(&models.MediaEpisode{}, id)
	}
	helpers.AppLogger.Infof("电视剧 %s 批次 %s 回滚全部完成，%d 条记录已重置为待刮削，将按新的识别信息自动重新刮削整理",
		mediaFile.Name, mediaFile.BatchNo, res.RowsAffected)
}

// 清理按旧识别结果整理出的目标目录（剧目录和季目录）。
// 安全约束：
//   - 仅刮削模式下目标目录就是源目录，绝不清理
//   - 目录仍被其他批次记录引用（如同一剧的其他季）时跳过，避免误删别的批次的文件
//   - 目录是（或包含）来源根目录时跳过，防止误删源
func (t *tvShowScrapeImpl) cleanupOldTargetDirs(batchFiles []*models.ScrapeMediaFile) {
	seasonDirs := make(map[string]string) // 目录ID => 完整路径
	showDirs := make(map[string]string)
	sourcePaths := make([]string, 0)
	for _, f := range batchFiles {
		if f.ScrapeType == models.ScrapeTypeOnly {
			// 仅刮削模式目标目录即源目录，不能删
			return
		}
		if f.NewSeasonPathId != "" {
			seasonDirs[f.NewSeasonPathId] = f.GetDestFullSeasonPath()
		}
		if f.NewPathId != "" {
			showDirs[f.NewPathId] = f.GetDestFullTvshowPath()
		}
		if f.SourcePath != "" {
			sourcePaths = append(sourcePaths, f.SourcePath)
		}
	}
	// 目标目录如果等于来源根目录、或来源根目录在其内部，则不能删
	unsafe := func(dirPath string) bool {
		for _, sp := range sourcePaths {
			if sp == dirPath || strings.HasPrefix(sp, dirPath+string(filepath.Separator)) || strings.HasPrefix(sp, dirPath+"/") {
				return true
			}
		}
		return false
	}
	for id, path := range seasonDirs {
		if path == "" || unsafe(path) {
			helpers.AppLogger.Warnf("旧目标季目录 %s 命中来源目录保护，跳过清理", path)
			continue
		}
		var inUse int64
		db.Db.Model(&models.ScrapeMediaFile{}).Where("new_season_path_id = ? AND batch_no != ?", id, batchFiles[0].BatchNo).Count(&inUse)
		if inUse > 0 {
			helpers.AppLogger.Infof("旧目标季目录 %s 仍被其他批次使用，跳过清理", path)
			continue
		}
		if err := t.renameImpl.DeleteDir(path, id); err != nil {
			helpers.AppLogger.Errorf("清理旧目标季目录 %s 失败: %v", path, err)
		} else {
			helpers.AppLogger.Infof("已清理旧目标季目录 %s", path)
		}
	}
	for id, path := range showDirs {
		if path == "" || unsafe(path) {
			helpers.AppLogger.Warnf("旧目标剧目录 %s 命中来源目录保护，跳过清理", path)
			continue
		}
		var inUse int64
		db.Db.Model(&models.ScrapeMediaFile{}).Where("new_path_id = ? AND batch_no != ?", id, batchFiles[0].BatchNo).Count(&inUse)
		if inUse > 0 {
			helpers.AppLogger.Infof("旧目标剧目录 %s 仍被其他批次使用，跳过清理", path)
			continue
		}
		if err := t.renameImpl.DeleteDir(path, id); err != nil {
			helpers.AppLogger.Errorf("清理旧目标剧目录 %s 失败: %v", path, err)
		} else {
			helpers.AppLogger.Infof("已清理旧目标剧目录 %s", path)
		}
	}
}

// 仅刮削的重新刮削逻辑：将对应刮削记录修改为待刮削
// 刮削和整理的重新刮削逻辑：
//   - 移动：将文件移动回源目录，如果源目录已删除，则新建同名目录并修改path、pathid等
//   - 复制：检查源目录和源视频文件是否依然存在，如果存在则删除目标目录，如果不存在则将目标文件移动回源目录（源目录不存在则新建），并修改videofileid, videofilename, videopickcode,pathid, pathname等值
//   - 软链接、硬链接：同复制
func (t *tvShowScrapeImpl) RollbackTvShow(mediaFile *models.ScrapeMediaFile) error {
	newBaseName := fmt.Sprintf("%s (%d) {tmdbid-%d}", mediaFile.Name, mediaFile.Year, mediaFile.TmdbId)
	// 如果是仅刮削则删除所有上传的元数据
	if mediaFile.ScrapeType == models.ScrapeTypeOnly {
		uploadFiles := t.GetTvshowUploadFiles(mediaFile)
		files := make([]models.WillDeleteFile, 0)
		for _, uf := range uploadFiles {
			files = append(files, models.WillDeleteFile{FullFilePath: filepath.Join(uf.DestPath, uf.FileName)})
		}
		// 删除这些文件
		err := t.renameImpl.CheckAndDeleteFiles(mediaFile, files)
		if err != nil {
			helpers.AppLogger.Errorf("删除已上传的元数据文失败: %v", err)
			return err
		}
		helpers.AppLogger.Infof("删除已上传的元数据文件成功: %v", files)
	}
	// 如果是刮削和整理或者仅整理
	if mediaFile.ScrapeType == models.ScrapeTypeScrapeAndRename || mediaFile.ScrapeType == models.ScrapeTypeOnlyRename {
		// 检查目录是否存在，如果存在则改名字，如果不存在则创建
		parentPath := filepath.Dir(mediaFile.TvshowPath)
		var newPath string
		var pathId string
		var existsPathId string = ""
		if mediaFile.TvshowPath == mediaFile.SourcePath {
			parentPath = mediaFile.SourcePath
			newPath = mediaFile.SourcePath
		} else {
			newPath = filepath.Join(parentPath, newBaseName)
		}
		if mediaFile.RenameType != models.RenameTypeMove && parentPath != mediaFile.SourcePath {
			// 先检查旧文件夹是否存在
			var eerr error
			existsPathId, eerr = t.renameImpl.ExistsAndRename(mediaFile.TvshowPathId, newBaseName)
			if eerr != nil {
				helpers.AppLogger.Errorf("重命名旧文件夹 %s 失败: %v", mediaFile.TvshowPathId, eerr)
				return eerr
			}
		}
		if existsPathId == "" {
			if parentPath != mediaFile.SourcePath {
				var err error
				pathId, err = t.renameImpl.CheckAndMkDir(newPath, mediaFile.SourcePath, mediaFile.SourcePathId)
				if err != nil {
					helpers.AppLogger.Errorf("创建父文件夹 %s 失败: %v", newPath, err)
					return err
				}
			} else {
				pathId = mediaFile.SourcePathId
				newPath = mediaFile.SourcePath
			}
		} else {
			pathId = existsPathId
		}
		mediaFile.TvshowPath = newPath
		mediaFile.TvshowPathId = pathId
		// 更新所有集的路径
		err := t.UpdateTvshowPathAndIdToAllEpisode(mediaFile)
		if err != nil {
			helpers.AppLogger.Errorf("更新电视剧 %s 的所有集的路径失败: %v", mediaFile.Name, err)
			return err
		}
	}
	// 将media设置为已回滚
	mediaFile.Media.Status = models.MediaStatusUnScraped
	mediaFile.Media.Save()
	helpers.AppLogger.Infof("回滚电视剧 %s 成功", mediaFile.Name)
	return nil
}

func (t *tvShowScrapeImpl) UpdateTvshowPathAndIdToAllEpisode(mediaFile *models.ScrapeMediaFile) error {
	// 更新电视剧下的所有集的数据
	// 如果是电视剧，则更新所有同剧的文件的NewPathId为新创建的文件夹ID
	// 批量更新
	// 将所有其他相同电视剧的季也修改信息
	// 构造更新数据
	updateData := map[string]interface{}{
		"tvshow_path_id": mediaFile.TvshowPathId,
		"tvshow_path":    mediaFile.TvshowPath,
	}
	// 批量更新
	affectedRows := db.Db.Table("scrape_media_files").Where("scrape_path_id =? AND media_id = ? AND batch_no = ?", mediaFile.ScrapePathId, mediaFile.MediaId, mediaFile.BatchNo).Updates(updateData).RowsAffected
	if affectedRows == 0 {
		helpers.AppLogger.Errorf("批量更新电视剧 %s 的所有集的信息失败, 未更新任何行", mediaFile.Name)
		return errors.New("no rows affected")
	}
	helpers.AppLogger.Infof("批量更新电视剧 %s 的所有集的信息成功，共更新 %d 行", mediaFile.Name, affectedRows)
	return nil
}
