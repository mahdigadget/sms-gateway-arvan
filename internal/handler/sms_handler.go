package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sms-gateway-arvan/internal/cache"
	kafkaPlatform "sms-gateway-arvan/internal/platform/kafka"
	"sms-gateway-arvan/internal/platform/telemetry"
	"time"

	"github.com/gin-gonic/gin"
)

// Request⟶Validation⟶Idempotency Key⟶Redis Reserve
//Success
//
// Kafka
//Produce
//بعدش همان جواب روی کلید idempotency ذخیره می‌شود
//Failure
//
// Redis
//Rollback
//
// processing duplicate⟶409
// completed duplicate⟶همان 202

type SMSHandler struct {
	balanceClient *cache.BalanceClient
	producerPool  *kafkaPlatform.ProducerPool
}

func NewSMSHandler(balanceClient *cache.BalanceClient, producerPool *kafkaPlatform.ProducerPool) *SMSHandler {
	return &SMSHandler{
		balanceClient: balanceClient,
		producerPool:  producerPool,
	}
}

// مدل‌های ورودی
type SendVIPRequest struct {
	TenantID    string `json:"tenant_id" binding:"required"`
	Destination string `json:"destination" binding:"required"`
	Sender      string `json:"sender" binding:"required"`
	Body        string `json:"body" binding:"required"`
	DeadlineAt  int64  `json:"deadline_at" binding:"required,gt=0"` // ثانیه، مثلاً ۲
}

type SendNormalRequest struct {
	TenantID    string `json:"tenant_id" binding:"required"`
	Destination string `json:"destination" binding:"required"`
	Sender      string `json:"sender" binding:"required"`
	Body        string `json:"body" binding:"required"`
}

// SendVIP: مدیریت درخواست‌های با اولویت بالا و تحویل حساس به زمان
func (h *SMSHandler) SendVIP(c *gin.Context) {
	// مبدأ مهلت: لحظهٔ رسیدن درخواست، نه زمان produce
	receivedAt := time.Now().UTC()

	var req SendVIPRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload", "details": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// تولید کلید Idempotency
	idempHash := cache.GenerateIdempotencyKey(req.TenantID, req.Destination, req.Sender, req.Body)

	// رزرو اتمیک اعتبار در ردیس
	if err := h.balanceClient.Reserve(ctx, req.TenantID, idempHash); err != nil {
		handleReserveError(c, err)
		return
	}

	// بررسی نهنگ بودن جهت اعمال سالتین بر روی پارتیشن‌ها
	isWhale, _ := h.balanceClient.IsWhale(ctx, req.TenantID)

	// ارسال به صف کافکا (acks=all)
	payload := kafkaPlatform.VIPMessagePayload{
		TenantID:    req.TenantID,
		Destination: req.Destination,
		Sender:      req.Sender,
		Body:        req.Body,
		DeadlineAt:  req.DeadlineAt,
		CreatedAt:   receivedAt,
		IdempHash:   idempHash,
	}
	// قطع شدن کلاینت نباید رول‌بک یا ذخیرهٔ جواب را کنسل کند
	redisCtx := context.WithoutCancel(ctx)
	if err := h.producerPool.ProduceVIP(ctx, payload, isWhale); err != nil {
		// فقط وقتی بروکر ack نداده پول برمی‌گردد
		if rbErr := h.balanceClient.Rollback(redisCtx, req.TenantID, idempHash); rbErr != nil {
			telemetry.Printf(ctx, "vip rollback failed: %v", rbErr)
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue vip message, transaction rolled back"})
		return
	}
	resp := gin.H{
		"status":  "queued",
		"type":    "vip",
		"idemp":   idempHash,
		"tracked": true,
	}
	// جواب را روی کلید می‌نویسیم تا تکرار همان 202 را بگیرد
	// اگر این ست fail شود، خود این درخواست باز هم 202 است چون کافکا قبول کرده
	_ = h.balanceClient.StoreResponse(redisCtx, req.TenantID, idempHash, http.StatusAccepted, resp)
	c.JSON(http.StatusAccepted, resp)
}

// نگاشت خطاهای رزرو به کدهای HTTP متناظر
func handleReserveError(c *gin.Context, err error) {
	var replay *cache.ReplayResponse
	if errors.As(err, &replay) {
		var payload map[string]interface{}
		if json.Unmarshal(replay.Body, &payload) != nil {
			c.JSON(http.StatusConflict, gin.H{"error": "duplicate request detected"})
			return
		}
		c.JSON(replay.HTTPStatus, payload)
		return
	}

	switch {
	case errors.Is(err, cache.ErrRequestInProgress):
		c.JSON(http.StatusConflict, gin.H{"error": "request in progress"})
	case errors.Is(err, cache.ErrDuplicateRequest):
		c.JSON(http.StatusConflict, gin.H{"error": "duplicate request detected"})
	case errors.Is(err, cache.ErrCacheNotReady):
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "account cache warming up, retry shortly"})
	case errors.Is(err, cache.ErrInsufficientFund):
		c.JSON(http.StatusPaymentRequired, gin.H{"error": "insufficient wallet balance"})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "balance reservation failed", "details": err.Error()})
	}
}

// SendNormal: مدیریت پیام‌های استاندارد با کارایی شبکه بالا (High Throughput)
func (h *SMSHandler) SendNormal(c *gin.Context) {
	var req SendNormalRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request payload", "details": err.Error()})
		return
	}

	ctx := c.Request.Context()

	// کلید Idempotency برای پیام‌های نرمال
	idempHash := cache.GenerateIdempotencyKey(req.TenantID, req.Destination, req.Sender, req.Body)

	// رزرو اعتبار
	if err := h.balanceClient.Reserve(ctx, req.TenantID, idempHash); err != nil {
		handleReserveError(c, err)
		return
	}

	// وضعیت نهنگ
	isWhale, _ := h.balanceClient.IsWhale(ctx, req.TenantID)

	// ارسال به کافکا
	payload := kafkaPlatform.NormalMessagePayload{
		TenantID:    req.TenantID,
		Destination: req.Destination,
		Sender:      req.Sender,
		Body:        req.Body,
		IdempHash:   idempHash,
	}

	redisCtx := context.WithoutCancel(ctx)
	if err := h.producerPool.ProduceNormal(ctx, payload, isWhale); err != nil {
		if rbErr := h.balanceClient.Rollback(redisCtx, req.TenantID, idempHash); rbErr != nil {
			telemetry.Printf(ctx, "normal rollback failed: %v", rbErr)
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue message, transaction rolled back"})
		return
	}

	resp := gin.H{
		"status": "queued",
		"type":   "normal",
		"idemp":  idempHash,
	}
	// جواب را روی کلید می‌نویسیم تا تکرار همان 202 را بگیرد
	_ = h.balanceClient.StoreResponse(redisCtx, req.TenantID, idempHash, http.StatusAccepted, resp)
	c.JSON(http.StatusAccepted, resp)
}
