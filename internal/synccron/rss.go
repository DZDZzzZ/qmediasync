package synccron

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"Q115-STRM/internal/helpers"
	"Q115-STRM/internal/models"
)

// RSS 2.0 结构（兼容 mikan 等动漫站）
type rssFeed struct {
	Channel struct {
		Items []rssItem `xml:"item"`
	} `xml:"channel"`
}

type rssItem struct {
	Title     string `xml:"title"`
	Link      string `xml:"link"`
	Guid      string `xml:"guid"`
	PubDate   string `xml:"pubDate"`
	Enclosure struct {
		Url  string `xml:"url,attr"`
		Type string `xml:"type,attr"`
	} `xml:"enclosure"`
	Description string `xml:"description"`
}

// getRssHttpClient 带代理的外部 HTTP 客户端（RSS 源可能在境外，遵循系统代理设置）
func getRssHttpClient() *http.Client {
	transport := &http.Transport{}
	if models.SettingsGlobal.HttpProxy != "" {
		if proxyUrl, err := url.Parse(models.SettingsGlobal.HttpProxy); err == nil {
			transport.Proxy = http.ProxyURL(proxyUrl)
		}
	}
	return &http.Client{
		Timeout:   60 * time.Second,
		Transport: transport,
	}
}

// FetchRssItems 拉取并解析 RSS，返回条目列表（按发布顺序）
func FetchRssItems(rssUrl string) ([]rssItem, error) {
	client := getRssHttpClient()
	req, err := http.NewRequest("GET", rssUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) QMediaSync-RSS/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求RSS失败: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("请求RSS返回状态码 %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return nil, fmt.Errorf("读取RSS内容失败: %v", err)
	}
	var feed rssFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("解析RSS失败: %v", err)
	}
	return feed.Channel.Items, nil
}

// extractMagnet 从 RSS 条目中提取磁力链接：优先 enclosure，其次 link/description 中的 magnet: 开头内容
func extractMagnet(item rssItem) string {
	if strings.HasPrefix(strings.ToLower(item.Enclosure.Url), "magnet:") {
		return item.Enclosure.Url
	}
	for _, candidate := range []string{item.Link, item.Guid, item.Description} {
		lower := strings.ToLower(candidate)
		idx := strings.Index(lower, "magnet:")
		if idx >= 0 {
			// 截到空白或 HTML 标签为止
			end := len(candidate)
			for i := idx; i < len(candidate); i++ {
				ch := candidate[i]
				if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '<' || ch == '"' || ch == '\'' {
					end = i
					break
				}
			}
			return candidate[idx:end]
		}
	}
	return ""
}

// itemGuid 生成条目去重标识：优先 guid，其次磁力链接，最后标题
func itemGuid(item rssItem, magnet string) string {
	if item.Guid != "" {
		return item.Guid
	}
	if magnet != "" {
		return magnet
	}
	return item.Title
}

// CheckDueRssSubscriptions 定时入口：检查所有到达检查时间的启用订阅
func CheckDueRssSubscriptions() {
	subs := models.GetEnabledRssSubscriptions()
	if len(subs) == 0 {
		return
	}
	now := time.Now().Unix()
	for _, sub := range subs {
		interval := int64(sub.CheckInterval)
		if interval < 5 {
			interval = 30
		}
		if sub.LastCheckAt > 0 && now-sub.LastCheckAt < interval*60 {
			continue
		}
		CheckRssSubscription(sub)
	}
}

// CheckRssSubscription 检查单个订阅：解析 RSS -> 去重 -> 115 离线下载 -> 记录
// 返回新增离线任务数量（手动触发接口也用这个返回值）
func CheckRssSubscription(sub *models.RssSubscription) int {
	helpers.AppLogger.Infof("开始检查RSS订阅 %s (%s)", sub.Name, sub.RssUrl)
	sub.LastCheckAt = time.Now().Unix()
	defer sub.Save()

	scrapePath := models.GetScrapePathByID(sub.ScrapePathId)
	if scrapePath == nil {
		sub.LastError = "关联的刮削目录不存在，请重新选择"
		helpers.AppLogger.Errorf("RSS订阅 %s 关联的刮削目录 %d 不存在", sub.Name, sub.ScrapePathId)
		return 0
	}
	if scrapePath.SourceType != models.SourceType115 {
		sub.LastError = "RSS离线下载仅支持115网盘的刮削目录"
		helpers.AppLogger.Errorf("RSS订阅 %s 关联的刮削目录不是115网盘", sub.Name)
		return 0
	}

	items, err := FetchRssItems(sub.RssUrl)
	if err != nil {
		sub.LastError = err.Error()
		helpers.AppLogger.Errorf("RSS订阅 %s 拉取失败: %v", sub.Name, err)
		return 0
	}
	helpers.AppLogger.Infof("RSS订阅 %s 拉取到 %d 个条目", sub.Name, len(items))

	// 收集未处理过的磁力链接
	type newItem struct {
		guid   string
		title  string
		magnet string
	}
	newItems := make([]newItem, 0)
	for _, item := range items {
		magnet := extractMagnet(item)
		if magnet == "" {
			continue
		}
		guid := itemGuid(item, magnet)
		if models.RssRecordExists(sub.ID, guid) {
			continue
		}
		newItems = append(newItems, newItem{guid: guid, title: item.Title, magnet: magnet})
		// 单次最多提交 10 个新任务，防止首次添加订阅时一次性灌入过多
		if len(newItems) >= 10 {
			break
		}
	}
	if len(newItems) == 0 {
		sub.LastError = ""
		helpers.AppLogger.Infof("RSS订阅 %s 没有新条目", sub.Name)
		return 0
	}

	// 初始化115客户端
	if !scrapePath.Init() {
		sub.LastError = "初始化115客户端失败，请检查网盘账号授权"
		helpers.AppLogger.Errorf("RSS订阅 %s 初始化115客户端失败", sub.Name)
		return 0
	}
	client := scrapePath.V115Client
	if client == nil {
		sub.LastError = "115客户端不可用，请检查网盘账号授权"
		return 0
	}

	// 批量提交离线下载（保存到刮削目录的来源目录）
	magnets := make([]string, 0, len(newItems))
	for _, ni := range newItems {
		magnets = append(magnets, ni.magnet)
	}
	results, err := client.AddOfflineTaskUrls(context.Background(), magnets, scrapePath.SourcePathId)
	if err != nil {
		sub.LastError = "提交115离线下载失败: " + err.Error()
		helpers.AppLogger.Errorf("RSS订阅 %s 提交115离线下载失败: %v", sub.Name, err)
		return 0
	}

	added := 0
	for i, result := range results {
		ni := newItems[i]
		record := &models.RssDownloadRecord{
			SubscriptionId: sub.ID,
			Guid:           ni.guid,
			Title:          ni.title,
			Magnet:         ni.magnet,
			InfoHash:       result.InfoHash,
		}
		if result.State {
			record.Status = models.RssRecordStatusAdded
			sub.LastItemTitle = ni.title
			added++
			helpers.AppLogger.Infof("RSS订阅 %s 已提交离线下载: %s (info_hash=%s)", sub.Name, ni.title, result.InfoHash)
		} else {
			record.Status = models.RssRecordStatusAddError
			record.ErrorMsg = fmt.Sprintf("Code=%d %s", result.Code, result.Message)
			helpers.AppLogger.Errorf("RSS订阅 %s 提交离线下载失败: %s Code=%d %s", sub.Name, ni.title, result.Code, result.Message)
		}
		if err := record.Save(); err != nil {
			helpers.AppLogger.Errorf("保存RSS下载记录失败: %v", err)
		}
	}
	sub.LastError = ""
	helpers.AppLogger.Infof("RSS订阅 %s 本次新增 %d 个离线下载任务", sub.Name, added)
	return added
}

// PollPendingRssRecords 轮询已提交的离线下载任务，完成后触发对应刮削目录的整理
func PollPendingRssRecords() {
	records := models.GetPendingRssRecords()
	if len(records) == 0 {
		return
	}
	// 按订阅分组，同一订阅共用一个115客户端和一次任务列表查询
	subCache := make(map[uint]*models.RssSubscription)
	clientCache := make(map[uint]*struct {
		scrapePath *models.ScrapePath
		tasks      map[string]int // info_hash -> status
	})

	triggerPaths := make(map[uint]bool)

	for _, record := range records {
		if record.InfoHash == "" {
			// 提交时没拿到 info_hash 的无法跟踪，直接标记成功（文件大概率已入库，由刮削定时任务兜底）
			record.Status = models.RssRecordStatusSuccess
			record.FinishedAt = time.Now().Unix()
			record.Save()
			continue
		}
		cacheKey := record.SubscriptionId
		if _, ok := clientCache[cacheKey]; !ok {
			sub := models.GetRssSubscriptionByID(record.SubscriptionId)
			if sub == nil {
				continue
			}
			subCache[cacheKey] = sub
			scrapePath := models.GetScrapePathByID(sub.ScrapePathId)
			if scrapePath == nil || scrapePath.SourceType != models.SourceType115 {
				continue
			}
			if !scrapePath.Init() || scrapePath.V115Client == nil {
				helpers.AppLogger.Errorf("RSS轮询：初始化115客户端失败，订阅 %s", sub.Name)
				continue
			}
			// 拉取任务列表（前2页足够覆盖近期任务）
			taskStatus := make(map[string]int)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			for page := 1; page <= 2; page++ {
				tasks, err := scrapePath.V115Client.GetOfflineTaskList(ctx, page)
				if err != nil {
					helpers.AppLogger.Errorf("RSS轮询：获取115离线任务列表失败: %v", err)
					break
				}
				for _, t := range tasks {
					taskStatus[t.InfoHash] = t.Status
				}
				if len(tasks) == 0 {
					break
				}
			}
			cancel()
			clientCache[cacheKey] = &struct {
				scrapePath *models.ScrapePath
				tasks      map[string]int
			}{scrapePath: scrapePath, tasks: taskStatus}
		}
		cache := clientCache[cacheKey]
		status, found := cache.tasks[record.InfoHash]
		if !found {
			// 任务列表里找不到了：115 对完成的任务有时会从列表移除。
			// 提交超过30分钟仍查不到，按成功处理（文件应已在目录中，交给刮削任务识别）
			if time.Now().Unix()-record.CreatedAt > 1800 {
				record.Status = models.RssRecordStatusSuccess
				record.FinishedAt = time.Now().Unix()
				record.Save()
				sub := subCache[cacheKey]
				helpers.AppLogger.Infof("RSS订阅 %s 的离线任务已从115任务列表消失，按完成处理: %s", sub.Name, record.Title)
				triggerPaths[sub.ScrapePathId] = true
			}
			continue
		}
		switch status {
		case 2: // 下载成功
			record.Status = models.RssRecordStatusSuccess
			record.FinishedAt = time.Now().Unix()
			record.Save()
			sub := subCache[cacheKey]
			helpers.AppLogger.Infof("RSS订阅 %s 离线下载完成: %s，将触发刮削整理", sub.Name, record.Title)
			triggerPaths[sub.ScrapePathId] = true
		case -1: // 下载失败
			record.Status = models.RssRecordStatusFailed
			record.ErrorMsg = "115离线下载失败"
			record.Save()
			helpers.AppLogger.Errorf("RSS订阅 %s 离线下载失败: %s", subCache[cacheKey].Name, record.Title)
		}
		// 0/1（分配中/下载中）继续等待
	}

	// 每个刮削目录只触发一次整理任务
	for scrapePathId := range triggerPaths {
		scrapePath := models.GetScrapePathByID(scrapePathId)
		if scrapePath == nil {
			continue
		}
		taskObj := &NewSyncTask{
			ID:         scrapePath.ID,
			AccountId:  scrapePath.AccountId,
			SourceType: scrapePath.SourceType,
			IsFile:     false,
			TaskType:   SyncTaskTypeScrape,
		}
		if err := AddNewSyncTask(taskObj); err != nil {
			// 任务已在队列中也算正常
			helpers.AppLogger.Warnf("RSS触发刮削目录 %s 整理任务入队失败（可能已在队列中）: %v", scrapePath.SourcePath, err)
		} else {
			helpers.AppLogger.Infof("RSS离线下载完成，已触发刮削目录 %s 的整理任务", scrapePath.SourcePath)
		}
	}
}
