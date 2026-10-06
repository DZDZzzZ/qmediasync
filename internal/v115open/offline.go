package v115open

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"Q115-STRM/internal/helpers"
)

// 115 开放平台云下载（离线）任务状态
const (
	OfflineTaskStatusFailed      = -1 // 下载失败
	OfflineTaskStatusAssigning   = 0  // 分配中
	OfflineTaskStatusDownloading = 1  // 下载中
	OfflineTaskStatusSuccess     = 2  // 下载成功
)

// OfflineAddResult 单个链接的离线任务添加结果
type OfflineAddResult struct {
	State    bool   `json:"state"`
	Code     int    `json:"code"`
	Message  string `json:"message"`
	InfoHash string `json:"info_hash"`
	Url      string `json:"url"`
}

// OfflineTask 云下载任务
type OfflineTask struct {
	InfoHash   string `json:"info_hash"`
	AddTime    int64  `json:"add_time"`
	Percent    int    `json:"percentDone"`
	Size       int64  `json:"size"`
	Name       string `json:"name"`
	LastUpdate int64  `json:"last_update"`
	FileId     string `json:"file_id"`
	Status     int    `json:"status"`
	Url        string `json:"url"`
	WpPathId   string `json:"wp_path_id"`
}

type offlineTaskListData struct {
	Page      int            `json:"page"`
	PageCount int            `json:"page_count"`
	Count     int            `json:"count"`
	Tasks     []OfflineTask  `json:"tasks"`
}

// AddOfflineTaskUrls 添加云下载链接任务（磁力/HTTP/电驴等），保存到 wpPathId 指定的文件夹。
// 返回每个链接的添加结果（与传入顺序一致）。
func (c *OpenClient) AddOfflineTaskUrls(ctx context.Context, urls []string, wpPathId string) ([]OfflineAddResult, error) {
	if len(urls) == 0 {
		return nil, fmt.Errorf("链接列表为空")
	}
	if len(urls) > 115 {
		return nil, fmt.Errorf("单批链接数量不能超过115个")
	}
	data := map[string]string{
		"urls":        strings.Join(urls, "\n"),
		"wp_path_id":  wpPathId,
	}
	url := fmt.Sprintf("%s/open/offline/add_task_urls", OPEN_BASE_URL)
	req := c.client.R().SetFormData(data).SetMethod("POST")
	var respData []OfflineAddResult
	_, bodyBytes, err := c.doAuthRequest(ctx, url, req, MakeRequestConfig(3, 1, 60), &respData)
	if err != nil {
		helpers.V115Log.Errorf("添加云下载任务失败: %v", err)
		return nil, err
	}
	// data 解包失败时（例如整体 code 非 0），从原始响应中取错误信息
	if respData == nil {
		resp := &RespBase[json.RawMessage]{}
		_ = json.Unmarshal(bodyBytes, &resp)
		return nil, fmt.Errorf("添加云下载任务失败: Code=%d, Message=%s", resp.Code, resp.Message)
	}
	return respData, nil
}

// GetOfflineTaskList 分页获取云下载任务列表
func (c *OpenClient) GetOfflineTaskList(ctx context.Context, page int) ([]OfflineTask, error) {
	if page < 1 {
		page = 1
	}
	url := fmt.Sprintf("%s/open/offline/get_task_list?page=%d", OPEN_BASE_URL, page)
	req := c.client.R().SetMethod("GET")
	var respData offlineTaskListData
	_, _, err := c.doAuthRequest(ctx, url, req, MakeRequestConfig(3, 1, 60), &respData)
	if err != nil {
		helpers.V115Log.Errorf("获取云下载任务列表失败: %v", err)
		return nil, err
	}
	return respData.Tasks, nil
}

// FindOfflineTaskByInfoHash 在任务列表前几页中查找指定 info_hash 的任务
func (c *OpenClient) FindOfflineTaskByInfoHash(ctx context.Context, infoHash string) (*OfflineTask, error) {
	// 只查前 3 页，避免频繁调用
	for page := 1; page <= 3; page++ {
		tasks, err := c.GetOfflineTaskList(ctx, page)
		if err != nil {
			return nil, err
		}
		for i := range tasks {
			if tasks[i].InfoHash == infoHash {
				return &tasks[i], nil
			}
		}
		if len(tasks) == 0 {
			break
		}
	}
	return nil, nil
}
