package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrDuplicateRequest  = errors.New("duplicate request")
	ErrRequestInProgress = errors.New("request in progress") // هنوز processing است و جواب نهایی نداریم
	ErrCacheNotReady     = errors.New("wallet cache not ready, sync in progress")
	ErrInsufficientFund  = errors.New("insufficient balance")
	ErrUnknownLuaStatus  = errors.New("unexpected status from redis script")
)

// ReplayResponse جواب ذخیره‌شدهٔ درخواست قبلی است
type ReplayResponse struct {
	HTTPStatus int
	Body       []byte
}

func (e *ReplayResponse) Error() string {
	return "idempotent replay"
}

const (
	DefaultCost    = int64(1)
	IdempTTLSecond = 24 * 60 * 60 // ۲۴ ساعت، هم‌قد کلید tele:dispatched
	WhalesSetKey   = "whales:set"
	HoldsKey       = "holds:processing"
)

type BalanceClient struct {
	rdb         *redis.Client
	reserveSHA  string
	rollbackSHA string
	commitSHA   string
	storeSHA    string
	refundSHA   string
}

func NewBalanceClient(rdb *redis.Client) *BalanceClient {
	ctx := context.Background()
	// لود کردن اسکریپت‌ها در حافظه ردیس برای اجرای سریع‌تر با EVALSHA
	rSHA, _ := rdb.ScriptLoad(ctx, ReserveScript).Result()
	rbSHA, _ := rdb.ScriptLoad(ctx, RollbackScript).Result()
	cSHA, _ := rdb.ScriptLoad(ctx, CommitScript).Result()
	sSHA, _ := rdb.ScriptLoad(ctx, StoreScript).Result()
	rfSHA, _ := rdb.ScriptLoad(ctx, RefundScript).Result()

	return &BalanceClient{
		rdb:         rdb,
		reserveSHA:  rSHA,
		rollbackSHA: rbSHA,
		commitSHA:   cSHA,
		storeSHA:    sSHA,
		refundSHA:   rfSHA,
	}
}

// GenerateIdempotencyKey: ساخت هش منحصر‌به‌فرد از محتوای پیام برای بررسی تکراری بودن
func GenerateIdempotencyKey(tenantID, destination, sender, body string) string {
	raw := fmt.Sprintf("%s:%s:%s:%s", tenantID, destination, sender, body)
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:16]) // 16تا کافیه
}

// Reserve: کسر از available و اضافه به reserved به صورت اتمیک
func (c *BalanceClient) Reserve(ctx context.Context, tenantID, idempHash string) error {
	walletKey := fmt.Sprintf("wallet:%s", tenantID)
	idempKey := fmt.Sprintf("idemp:%s:%s", tenantID, idempHash)
	reservedAt := time.Now().Unix()

	res, err := c.rdb.EvalSha(ctx, c.reserveSHA, []string{walletKey, idempKey}, DefaultCost, IdempTTLSecond, reservedAt).Result()
	if err != nil {
		// در صورت عدم وجود اسکریپت در کش ردیس (NOSCRIPT)، fallback به Eval معمولی
		res, err = c.rdb.Eval(ctx, ReserveScript, []string{walletKey, idempKey}, DefaultCost, IdempTTLSecond, reservedAt).Result()
		if err != nil {
			return fmt.Errorf("redis execution error: %w", err)
		}
	}

	return mapReserveResult(res)
}

// StoreResponse جواب قبول‌شده را جای processing می‌نویسد تا تکرار همان پاسخ را بگیرد
func (c *BalanceClient) StoreResponse(ctx context.Context, tenantID, idempHash string, httpStatus int, body interface{}) error {
	envelope, err := json.Marshal(map[string]interface{}{
		"http_status": httpStatus,
		"body":        body,
	})
	if err != nil {
		return err
	}

	idempKey := fmt.Sprintf("idemp:%s:%s", tenantID, idempHash)
	_, err = c.rdb.EvalSha(ctx, c.storeSHA, []string{idempKey}, string(envelope), IdempTTLSecond).Result()
	if err != nil {
		// NOSCRIPT یا هر خطای کش اسکریپت، یک بار با خود متن اسکریپت
		return c.rdb.Eval(ctx, StoreScript, []string{idempKey}, string(envelope), IdempTTLSecond).Err()
	}
	return nil
}

func mapReserveResult(res interface{}) error {
	switch v := res.(type) {
	case string:
		return replayFromStored(v)
	case int64:
		switch v {
		case 1:
			return nil
		case -1:
			return ErrRequestInProgress
		case -2:
			return ErrCacheNotReady
		case -3:
			return ErrInsufficientFund
		default:
			return ErrUnknownLuaStatus
		}
	default:
		return ErrUnknownLuaStatus
	}
}

func replayFromStored(raw string) error {
	var stored struct {
		HTTPStatus int             `json:"http_status"`
		Body       json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal([]byte(raw), &stored); err != nil || stored.HTTPStatus == 0 || len(stored.Body) == 0 {
		// کلید قدیمی که مقدارش "1" بوده هنوز 409 می‌ماند
		return ErrDuplicateRequest
	}
	return &ReplayResponse{HTTPStatus: stored.HTTPStatus, Body: stored.Body}
}

// Rollback: برگرداندن موجودی در صورت بروز خطا در ارسال به کافکا
func (c *BalanceClient) Rollback(ctx context.Context, tenantID, idempHash string) error {
	walletKey := fmt.Sprintf("wallet:%s", tenantID)
	idempKey := fmt.Sprintf("idemp:%s:%s", tenantID, idempHash)

	_, err := c.rdb.EvalSha(ctx, c.rollbackSHA, []string{walletKey, idempKey, HoldsKey}, DefaultCost).Result()
	if err != nil {
		return c.rdb.Eval(ctx, RollbackScript, []string{walletKey, idempKey, HoldsKey}, DefaultCost).Err()
	}
	return nil
}

// Commit: کسر از reserved بعد از اینکه پیام در پایگاه داده حسابداری ثبت شد.
// برای هر idemp یک بار اجرا می‌شود.
func (c *BalanceClient) Commit(ctx context.Context, tenantID, idempHash string) error {
	walletKey := fmt.Sprintf("wallet:%s", tenantID)
	doneKey := fmt.Sprintf("acct:done:%s", idempHash)
	idempKey := fmt.Sprintf("idemp:%s:%s", tenantID, idempHash)

	_, err := c.rdb.EvalSha(ctx, c.commitSHA, []string{walletKey, doneKey, HoldsKey}, DefaultCost, IdempTTLSecond, idempKey).Result()
	if err != nil {
		return c.rdb.Eval(ctx, CommitScript, []string{walletKey, doneKey, HoldsKey}, DefaultCost, IdempTTLSecond, idempKey).Err()
	}
	return nil
}

// RefundAvailable برگشت مستقیم به available، یک بار برای هر idemp.
func (c *BalanceClient) RefundAvailable(ctx context.Context, tenantID, idempHash string, amount int64) error {
	if amount <= 0 {
		amount = DefaultCost
	}
	walletKey := fmt.Sprintf("wallet:%s", tenantID)
	onceKey := fmt.Sprintf("saga:refund:%s", idempHash)
	doneKey := fmt.Sprintf("acct:done:%s", idempHash)
	idempKey := fmt.Sprintf("idemp:%s:%s", tenantID, idempHash)

	_, err := c.rdb.EvalSha(ctx, c.refundSHA, []string{walletKey, onceKey, doneKey, HoldsKey}, amount, IdempTTLSecond, idempKey).Result()
	if err != nil {
		return c.rdb.Eval(ctx, RefundScript, []string{walletKey, onceKey, doneKey, HoldsKey}, amount, IdempTTLSecond, idempKey).Err()
	}
	return nil
}

// IsWhale: بررسی اینکه آیا tenant یک نهنگ است یا خیر
func (c *BalanceClient) IsWhale(ctx context.Context, tenantID string) (bool, error) {
	return c.rdb.SIsMember(ctx, WhalesSetKey, tenantID).Result()
}
