package models

import (
	"Q115-STRM/internal/db"
	"Q115-STRM/internal/helpers"
)

// RssSubscription RSS订阅，关联一个刮削目录：
// 定时检查 RSS 中的新磁力链接 -> 115 离线下载到该刮削目录的来源目录 -> 下载完成后触发该目录的刮削整理
type RssSubscription struct {
	BaseModel
	Name          string `json:"name" form:"name"`                       // 订阅名称，例如 航海王
	RssUrl        string `json:"rss_url" form:"rss_url"`                 // RSS 地址，例如 https://mikanani.kas.pub/RSS/Bangumi?bangumiId=3015&subgroupid=615
	ScrapePathId  uint   `json:"scrape_path_id" form:"scrape_path_id"`   // 关联的刮削目录ID，离线文件保存到该目录的来源路径，完成后触发其整理
	Enabled       bool   `json:"enabled" form:"enabled"`                 // 是否启用
	CheckInterval int    `json:"check_interval" form:"check_interval"`   // 检查间隔（分钟），默认30，最小5
	LastCheckAt   int64  `json:"last_check_at" form:"-"`                 // 上次检查时间戳（秒）
	LastError     string `json:"last_error" form:"-"`                    // 上次检查的错误信息，成功时清空
	LastItemTitle string `json:"last_item_title" form:"-"`               // 最近一次添加离线下载的条目标题，便于前端展示
}

// RSS 下载记录的状态
type RssRecordStatus string

const (
	RssRecordStatusAdded    RssRecordStatus = "added"    // 已提交115离线下载
	RssRecordStatusSuccess  RssRecordStatus = "success"  // 离线下载完成，已触发整理
	RssRecordStatusFailed   RssRecordStatus = "failed"   // 离线下载失败
	RssRecordStatusAddError RssRecordStatus = "add_error" // 提交离线下载失败
)

// RssDownloadRecord 已处理的 RSS 条目记录，用于去重和跟踪离线下载进度
type RssDownloadRecord struct {
	BaseModel
	SubscriptionId uint            `json:"subscription_id" form:"subscription_id" gorm:"index:idx_rss_record_sub_guid,unique"` // 所属订阅
	Guid           string          `json:"guid" form:"guid" gorm:"index:idx_rss_record_sub_guid,unique"`                       // RSS 条目唯一标识（guid 或磁力链接）
	Title          string          `json:"title" form:"title"`                                                                 // 条目标题
	Magnet         string          `json:"magnet" form:"magnet"`                                                               // 磁力链接
	InfoHash       string          `json:"info_hash" form:"info_hash" gorm:"index"`                                            // 115 离线任务 hash
	Status         RssRecordStatus `json:"status" form:"status"`                                                               // 状态
	ErrorMsg       string          `json:"error_msg" form:"error_msg"`                                                         // 错误信息
	FinishedAt     int64           `json:"finished_at" form:"finished_at"`                                                     // 离线下载完成时间戳（秒）
}

func (r *RssSubscription) Save() error {
	if r.CheckInterval < 5 {
		r.CheckInterval = 30
	}
	if r.ID == 0 {
		return db.Db.Create(r).Error
	}
	return db.Db.Model(r).Updates(map[string]any{
		"name":            r.Name,
		"rss_url":         r.RssUrl,
		"scrape_path_id":  r.ScrapePathId,
		"enabled":         r.Enabled,
		"check_interval":  r.CheckInterval,
		"last_check_at":   r.LastCheckAt,
		"last_error":      r.LastError,
		"last_item_title": r.LastItemTitle,
	}).Error
}

func (r *RssSubscription) Delete() error {
	// 同时删除该订阅的下载记录
	if err := db.Db.Where("subscription_id = ?", r.ID).Delete(&RssDownloadRecord{}).Error; err != nil {
		helpers.AppLogger.Errorf("删除RSS订阅 %d 的下载记录失败: %v", r.ID, err)
	}
	return db.Db.Delete(r).Error
}

func GetRssSubscriptions() []*RssSubscription {
	var subs []*RssSubscription
	if err := db.Db.Model(&RssSubscription{}).Order("id DESC").Find(&subs).Error; err != nil {
		helpers.AppLogger.Errorf("获取RSS订阅列表失败: %v", err)
	}
	return subs
}

func GetRssSubscriptionByID(id uint) *RssSubscription {
	var sub RssSubscription
	if err := db.Db.Model(&RssSubscription{}).Where("id = ?", id).First(&sub).Error; err != nil {
		return nil
	}
	return &sub
}

// GetEnabledRssSubscriptionsDue 获取启用的订阅（是否到达检查时间由调用方判断）
func GetEnabledRssSubscriptions() []*RssSubscription {
	var subs []*RssSubscription
	if err := db.Db.Model(&RssSubscription{}).Where("enabled = ?", true).Find(&subs).Error; err != nil {
		helpers.AppLogger.Errorf("获取启用的RSS订阅失败: %v", err)
	}
	return subs
}

func (r *RssDownloadRecord) Save() error {
	if r.ID == 0 {
		return db.Db.Create(r).Error
	}
	return db.Db.Model(r).Updates(map[string]any{
		"info_hash":   r.InfoHash,
		"status":      r.Status,
		"error_msg":   r.ErrorMsg,
		"finished_at": r.FinishedAt,
	}).Error
}

// RssRecordExists 判断条目是否已处理过（按 订阅ID+Guid 去重）
func RssRecordExists(subscriptionId uint, guid string) bool {
	var count int64
	db.Db.Model(&RssDownloadRecord{}).Where("subscription_id = ? AND guid = ?", subscriptionId, guid).Count(&count)
	return count > 0
}

// GetPendingRssRecords 获取已提交离线下载但尚未完成的记录
func GetPendingRssRecords() []*RssDownloadRecord {
	var records []*RssDownloadRecord
	if err := db.Db.Model(&RssDownloadRecord{}).
		Where("status = ?", RssRecordStatusAdded).
		Find(&records).Error; err != nil {
		helpers.AppLogger.Errorf("获取待完成的RSS下载记录失败: %v", err)
	}
	return records
}

// GetRssRecordsBySubscription 获取某订阅的下载记录（前端展示，按时间倒序）
func GetRssRecordsBySubscription(subscriptionId uint, limit int) []*RssDownloadRecord {
	var records []*RssDownloadRecord
	q := db.Db.Model(&RssDownloadRecord{}).Where("subscription_id = ?", subscriptionId).Order("id DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	if err := q.Find(&records).Error; err != nil {
		helpers.AppLogger.Errorf("获取RSS订阅 %d 的下载记录失败: %v", subscriptionId, err)
	}
	return records
}
