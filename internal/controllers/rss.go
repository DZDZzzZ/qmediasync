package controllers

import (
	"net/http"

	"Q115-STRM/internal/helpers"
	"Q115-STRM/internal/models"
	"Q115-STRM/internal/synccron"

	"github.com/gin-gonic/gin"
)

// GetRssSubscriptions 获取 RSS 订阅列表
// @Summary 获取RSS订阅列表
// @Tags RSS订阅
// @Router /api/rss/subscriptions [get]
func GetRssSubscriptions(c *gin.Context) {
	subs := models.GetRssSubscriptions()
	c.JSON(http.StatusOK, APIResponse[[]*models.RssSubscription]{Code: Success, Message: "", Data: subs})
}

// SaveRssSubscription 新增或更新 RSS 订阅
// @Summary 保存RSS订阅
// @Tags RSS订阅
// @Router /api/rss/subscriptions [post]
func SaveRssSubscription(c *gin.Context) {
	var req models.RssSubscription
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: err.Error(), Data: nil})
		return
	}
	if req.Name == "" || req.RssUrl == "" || req.ScrapePathId == 0 {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "名称、RSS地址、关联刮削目录不能为空", Data: nil})
		return
	}
	scrapePath := models.GetScrapePathByID(req.ScrapePathId)
	if scrapePath == nil {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "关联的刮削目录不存在", Data: nil})
		return
	}
	if scrapePath.SourceType != models.SourceType115 {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "RSS离线下载仅支持115网盘的刮削目录", Data: nil})
		return
	}
	if err := req.Save(); err != nil {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "保存失败: " + err.Error(), Data: nil})
		return
	}
	c.JSON(http.StatusOK, APIResponse[any]{Code: Success, Message: "保存成功", Data: nil})
}

// DeleteRssSubscription 删除 RSS 订阅
// @Summary 删除RSS订阅
// @Tags RSS订阅
// @Router /api/rss/subscriptions/:id [delete]
func DeleteRssSubscription(c *gin.Context) {
	id := helpers.StringToInt(c.Param("id"))
	sub := models.GetRssSubscriptionByID(uint(id))
	if sub == nil {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "订阅不存在", Data: nil})
		return
	}
	if err := sub.Delete(); err != nil {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "删除失败: " + err.Error(), Data: nil})
		return
	}
	c.JSON(http.StatusOK, APIResponse[any]{Code: Success, Message: "删除成功", Data: nil})
}

// CheckRssSubscriptionNow 手动立即检查一次 RSS 订阅
// @Summary 立即检查RSS订阅
// @Tags RSS订阅
// @Router /api/rss/subscriptions/:id/check [post]
func CheckRssSubscriptionNow(c *gin.Context) {
	id := helpers.StringToInt(c.Param("id"))
	sub := models.GetRssSubscriptionByID(uint(id))
	if sub == nil {
		c.JSON(http.StatusOK, APIResponse[any]{Code: BadRequest, Message: "订阅不存在", Data: nil})
		return
	}
	// 异步执行，避免前端等待整个 RSS 拉取+离线下载提交流程
	go synccron.CheckRssSubscription(sub)
	c.JSON(http.StatusOK, APIResponse[any]{Code: Success, Message: "已触发检查，请稍后刷新查看结果", Data: nil})
}

// GetRssSubscriptionRecords 获取某订阅的下载记录
// @Summary 获取RSS订阅的下载记录
// @Tags RSS订阅
// @Router /api/rss/subscriptions/:id/records [get]
func GetRssSubscriptionRecords(c *gin.Context) {
	id := helpers.StringToInt(c.Param("id"))
	records := models.GetRssRecordsBySubscription(uint(id), 100)
	c.JSON(http.StatusOK, APIResponse[[]*models.RssDownloadRecord]{Code: Success, Message: "", Data: records})
}
