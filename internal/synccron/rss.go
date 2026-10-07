package synccron

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
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
	// nyaa 系站把真正的 info hash 放在 <nyaa:infoHash> 扩展标签里（enclosure 是 download/<id>.torrent，不是 hash）。
	// tag 不写命名空间时 encoding/xml 按 local name 匹配，可兼容 nyaa: / 其他前缀。
	InfoHash string `xml:"infoHash"`
	// anibt 等站把磁力/哈希放在自定义 <torrent><magneturi>/<infohash></torrent> 里
	Torrent struct {
		InfoHash  string `xml:"infohash"`
		MagnetURI string `xml:"magneturi"`
	} `xml:"torrent"`
}

// rssSiteType RSS 站点类型，用于按站选择磁力提取方式（对齐 rss2cloud 的分站点逻辑）
type rssSiteType int

const (
	siteUnknown rssSiteType = iota
	siteMikan
	siteNyaa
	siteDmhy
	siteAcgnx
	siteRsshub
	siteAnibt
)

func (s rssSiteType) String() string {
	switch s {
	case siteMikan:
		return "mikan"
	case siteNyaa:
		return "nyaa"
	case siteDmhy:
		return "dmhy"
	case siteAcgnx:
		return "acgnx"
	case siteRsshub:
		return "rsshub"
	case siteAnibt:
		return "anibt"
	default:
		return "unknown"
	}
}

// detectSite 按 host 关键字识别站点。
// 用关键字而非精确 host：Mikan 等常见有镜像/代理域名（如 mikanani.kas.pub），
// rss2cloud 的精确匹配（mikanani.me/mikanime.tv）会漏掉这些镜像。
func detectSite(rssUrl string) rssSiteType {
	host := rssUrl
	if u, err := url.Parse(rssUrl); err == nil && u.Host != "" {
		host = u.Host
	}
	host = strings.ToLower(host)
	switch {
	case strings.Contains(host, "mikan"):
		return siteMikan
	case strings.Contains(host, "nyaa"):
		return siteNyaa
	case strings.Contains(host, "dmhy"):
		return siteDmhy
	case strings.Contains(host, "acgnx"):
		return siteAcgnx
	case strings.Contains(host, "rsshub"):
		return siteRsshub
	case strings.Contains(host, "anibt"):
		return siteAnibt
	default:
		return siteUnknown
	}
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

// extractMagnetFromText 从任意文本中截取 magnet: 链接（截到空白或 HTML 标签/引号为止）
func extractMagnetFromText(text string) string {
	idx := strings.Index(strings.ToLower(text), "magnet:")
	if idx < 0 {
		return ""
	}
	end := len(text)
	for i := idx; i < len(text); i++ {
		ch := text[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '<' || ch == '"' || ch == '\'' {
			end = i
			break
		}
	}
	return text[idx:end]
}

// infoHashRe 匹配 40 位十六进制 info hash（btih 常见形式）
var infoHashRe = regexp.MustCompile(`(?i)\b[a-f0-9]{40}\b`)

// deriveMagnetFromHash 从条目里推导 info hash 拼出磁力链接。
// Mikan 等站不给磁力，但种子文件名和 Episode 路径里都带 40 位 info hash：
//   - enclosure: https://.../Download/20261006/<hash>.torrent
//   - link/guid: https://.../Home/Episode/<hash>
//
// 参考 zhifengle/rss2cloud 的 Mikanani.GetMagnet（它直接用 Link 里 Episode/ 后那段当 btih）。
// 磁力比 .torrent 直链更可靠：直链带日期会过期，磁力永久，且 115 能按 info_hash 原生去重。
func deriveMagnetFromHash(item rssItem) string {
	for _, c := range []string{item.Enclosure.Url, item.Link, item.Guid} {
		if c == "" {
			continue
		}
		if h := infoHashRe.FindString(c); h != "" {
			return "magnet:?xt=urn:btih:" + strings.ToLower(h)
		}
	}
	return ""
}

// trimMagnetDN 去掉磁力链接尾部的 &dn=<显示名>（rss2cloud 各站也这么做，只保留 btih 主体）
func trimMagnetDN(magnet string) string {
	magnet = strings.TrimSpace(magnet)
	if i := strings.Index(magnet, "&dn="); i >= 0 {
		return magnet[:i]
	}
	return magnet
}

// buildMagnet 由 info hash 拼磁力（hash 需为 40 位十六进制，否则返回空）
func buildMagnet(hash string) string {
	hash = strings.TrimSpace(hash)
	if len(hash) == 40 && infoHashRe.MatchString(hash) {
		return "magnet:?xt=urn:btih:" + strings.ToLower(hash)
	}
	return ""
}

// mikanMagnet Mikan：Link 形如 .../Home/Episode/<40hex>，取该 hash 拼磁力（rss2cloud Mikanani.GetMagnet）。
// 兜底再从 enclosure 种子文件名/其它字段推 hash。
func mikanMagnet(item rssItem) string {
	if parts := strings.Split(item.Link, "Episode/"); len(parts) == 2 {
		if m := buildMagnet(parts[1]); m != "" {
			return m
		}
	}
	return deriveMagnetFromHash(item)
}

// nyaaMagnet Nyaa：读 <nyaa:infoHash> 扩展标签拼磁力（enclosure 是 download/<id>.torrent，不含真 hash）
func nyaaMagnet(item rssItem) string {
	if m := buildMagnet(item.InfoHash); m != "" {
		return m
	}
	return ""
}

// enclosureMagnet 取 enclosure：磁力直接用（去 &dn=），种子/直链 http(s) 原样返回（dmhy/rsshub 用）
func enclosureMagnet(item rssItem) string {
	enc := strings.TrimSpace(item.Enclosure.Url)
	if enc == "" {
		return ""
	}
	lower := strings.ToLower(enc)
	if strings.HasPrefix(lower, "magnet:") {
		return trimMagnetDN(enc)
	}
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return enc
	}
	return ""
}

// anibtMagnet anibt：优先自定义 <torrent><magneturi>，其次 <infohash>，最后 enclosure（rss2cloud Anibt.GetMagnet）
func anibtMagnet(item rssItem) string {
	if mu := strings.TrimSpace(item.Torrent.MagnetURI); strings.HasPrefix(strings.ToLower(mu), "magnet:") {
		return trimMagnetDN(mu)
	}
	if m := buildMagnet(item.Torrent.InfoHash); m != "" {
		return m
	}
	return enclosureMagnet(item)
}

// magnetForSite 按站点类型选专属提取方式，返回空表示该站没提取到，交给通用兜底
func magnetForSite(site rssSiteType, item rssItem) string {
	switch site {
	case siteMikan:
		return mikanMagnet(item)
	case siteNyaa:
		return nyaaMagnet(item)
	case siteDmhy, siteRsshub:
		return enclosureMagnet(item)
	case siteAcgnx:
		// acgnx：enclosure[0].URL 直接用（rss2cloud Acgnx.GetMagnet）
		return strings.TrimSpace(item.Enclosure.Url)
	case siteAnibt:
		return anibtMagnet(item)
	default:
		return ""
	}
}

// extractDownloadUrl 提取可用于 115 离线下载的链接。
// 先按站点专属逻辑提取（对齐 rss2cloud 的分站点解析）；站点未知或专属逻辑没提取到时，
// 退回通用启发式，避免镜像站/小众源直接失效。
func extractDownloadUrl(item rssItem, site rssSiteType) string {
	if m := magnetForSite(site, item); m != "" {
		return m
	}
	return genericDownloadUrl(item)
}

// genericDownloadUrl 通用启发式（站点未知时的兜底）：
// 正文磁力 -> 由 40 位 info hash 推导磁力 -> enclosure 的种子/直链 http(s)。
func genericDownloadUrl(item rssItem) string {
	enc := strings.TrimSpace(item.Enclosure.Url)
	if strings.HasPrefix(strings.ToLower(enc), "magnet:") {
		return trimMagnetDN(enc)
	}
	for _, candidate := range []string{item.Link, item.Guid, item.Description} {
		if m := extractMagnetFromText(candidate); m != "" {
			return trimMagnetDN(m)
		}
	}
	if m := deriveMagnetFromHash(item); m != "" {
		return m
	}
	lowerEnc := strings.ToLower(enc)
	if strings.HasPrefix(lowerEnc, "http://") || strings.HasPrefix(lowerEnc, "https://") {
		return enc
	}
	return ""
}

// matchAnyKeyword 标题命中任意一个关键词即返回 true（大小写不敏感，空关键词忽略）
func matchAnyKeyword(title string, keywords []string) bool {
	if len(keywords) == 0 {
		return false
	}
	lowerTitle := strings.ToLower(title)
	for _, kw := range keywords {
		kw = strings.TrimSpace(kw)
		if kw == "" {
			continue
		}
		if strings.Contains(lowerTitle, strings.ToLower(kw)) {
			return true
		}
	}
	return false
}

// parseExcludeFromUrl 从 RSS 地址的 #exclude= 片段解析排除关键词，返回去掉片段的干净地址与关键词。
// 例：https://.../RSS/Bangumi?bangumiId=227#exclude=【繁体】,【简体】
// 关键词支持中英文逗号、竖线、顿号分隔。没有 # 片段或不是 exclude= 时返回原地址与 nil。
func parseExcludeFromUrl(rssUrl string) (string, []string) {
	idx := strings.Index(rssUrl, "#")
	if idx < 0 {
		return rssUrl, nil
	}
	clean := rssUrl[:idx]
	fragment := rssUrl[idx+1:]
	const prefix = "exclude="
	if len(fragment) < len(prefix) || !strings.EqualFold(fragment[:len(prefix)], prefix) {
		return clean, nil
	}
	value := fragment[len(prefix):]
	var keywords []string
	for _, part := range strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == '，' || r == '|' || r == '、'
	}) {
		if part = strings.TrimSpace(part); part != "" {
			keywords = append(keywords, part)
		}
	}
	return clean, keywords
}

// mergeUniqueKeywords 合并多组关键词，去空、按小写去重，保持首次出现顺序
func mergeUniqueKeywords(lists ...[]string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, l := range lists {
		for _, kw := range l {
			kw = strings.TrimSpace(kw)
			if kw == "" || seen[strings.ToLower(kw)] {
				continue
			}
			seen[strings.ToLower(kw)] = true
			out = append(out, kw)
		}
	}
	return out
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

	// RSS 地址支持用 #exclude=关键词1,关键词2 片段配置排除词，
	// 这样在现有前端（无独立筛选框）下也能按每条订阅单独设置过滤规则
	fetchUrl, urlExcludeKeywords := parseExcludeFromUrl(sub.RssUrl)
	site := detectSite(fetchUrl)
	items, err := FetchRssItems(fetchUrl)
	if err != nil {
		sub.LastError = err.Error()
		helpers.AppLogger.Errorf("RSS订阅 %s 拉取失败: %v", sub.Name, err)
		return 0
	}
	helpers.AppLogger.Infof("RSS订阅 %s 拉取到 %d 个条目，识别站点类型: %s", sub.Name, len(items), site)

	// 排除关键词：DB 字段与 RSS 地址 #exclude= 片段合并，标题命中任意一个即跳过（例如【繁体】）
	excludeKeywords := mergeUniqueKeywords(sub.GetExcludeKeywords(), urlExcludeKeywords)
	if len(excludeKeywords) > 0 {
		helpers.AppLogger.Infof("RSS订阅 %s 排除关键词: %v", sub.Name, excludeKeywords)
	}

	// 收集未处理过、且带可用下载链接的条目
	type newItem struct {
		guid        string
		title       string
		downloadUrl string
	}
	newItems := make([]newItem, 0)
	skippedNoUrl := 0
	skippedExcluded := 0
	skippedDup := 0
	for _, item := range items {
		downloadUrl := extractDownloadUrl(item, site)
		if downloadUrl == "" {
			skippedNoUrl++
			continue
		}
		if matchAnyKeyword(item.Title, excludeKeywords) {
			skippedExcluded++
			continue
		}
		guid := itemGuid(item, downloadUrl)
		if models.RssRecordExists(sub.ID, guid) {
			skippedDup++
			continue
		}
		newItems = append(newItems, newItem{guid: guid, title: item.Title, downloadUrl: downloadUrl})
		// 单次最多提交 10 个新任务，防止首次添加订阅时一次性灌入过多
		if len(newItems) >= 10 {
			break
		}
	}
	helpers.AppLogger.Infof("RSS订阅 %s 条目统计: 共%d, 无可用下载链接%d, 命中排除词%d, 已处理过%d, 本次待提交%d",
		sub.Name, len(items), skippedNoUrl, skippedExcluded, skippedDup, len(newItems))
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
	downloadUrls := make([]string, 0, len(newItems))
	for _, ni := range newItems {
		downloadUrls = append(downloadUrls, ni.downloadUrl)
	}
	results, err := client.AddOfflineTaskUrls(context.Background(), downloadUrls, scrapePath.SourcePathId)
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
			Magnet:         ni.downloadUrl,
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
