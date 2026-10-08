package accounting

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sms-gateway-arvan/internal/cache"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// بیشتر از لگ حسابداری. اگر کوتاه‌تر باشد پیام هنوز توی تاپیک است،
	// پول برمی‌گردد و بعد پستگرس همان را کم می‌کند.
	holdMaxAge = 2 * time.Minute
)

type HoldSweeper struct {
	db  *sql.DB
	rdb *redis.Client
}

func NewHoldSweeper(db *sql.DB, rdb *redis.Client) *HoldSweeper {
	return &HoldSweeper{db: db, rdb: rdb}
}

func (s *HoldSweeper) Start(ctx context.Context) {
	s.pass(ctx) // بعد از قطع برق، همین که پروسه بالا آمد
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Printf("hold sweeper started (max age: %s)", holdMaxAge)

	for {
		select {
		case <-ctx.Done():
			log.Println("stopping hold sweeper")
			return
		case <-ticker.C:
			s.pass(ctx)
		}
	}
}

func (s *HoldSweeper) pass(ctx context.Context) {
	cutoff := strconv.FormatInt(time.Now().Add(-holdMaxAge).Unix(), 10)
	members, err := s.rdb.ZRangeByScore(ctx, cache.HoldsKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   cutoff,
		Count: 100,
	}).Result()
	if err != nil {
		log.Printf("[HOLD-SWEEPER] range failed: %v", err)
		return
	}

	for _, idempKey := range members {
		tenant, hash := splitIdempKey(idempKey)
		if tenant == "" || hash == "" {
			_ = s.rdb.ZRem(ctx, cache.HoldsKey, idempKey).Err()
			continue
		}
		walletKey := "wallet:" + tenant

		var eventType string
		err = s.db.QueryRowContext(ctx,
			`SELECT event_type FROM outbox_events WHERE idempotency_key = $1`,
			hash,
		).Scan(&eventType)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.Printf("[HOLD-SWEEPER] outbox lookup failed for %s: %v", hash, err)
			continue
		}
		if err == nil && eventType != "message.rejected" {
			// accepted یا refunded مال حسابداری است. پول را برنمی‌گردانیم.
			_ = s.rdb.ZRem(ctx, cache.HoldsKey, idempKey).Err()
			continue
		}
		if eventType == "message.rejected" {
			// tombstone نشسته ولی رزرو شاید مانده باشد. Rollback فقط اگر هنوز processing باشد.
			_, err = s.rdb.Eval(ctx, cache.RollbackScript,
				[]string{walletKey, idempKey, cache.HoldsKey},
				cache.DefaultCost,
			).Result()
			if err != nil {
				log.Printf("[HOLD-SWEEPER] rollback rejected hold failed for %s: %v", idempKey, err)
			}
			continue
		}

		res, err := s.rdb.Eval(ctx, cache.ExpireHoldScript,
			[]string{walletKey, idempKey, cache.HoldsKey},
			cache.DefaultCost, idempKey,
		).Result()
		if err != nil {
			log.Printf("[HOLD-SWEEPER] expire failed for %s: %v", idempKey, err)
			continue
		}
		if code, ok := res.(int64); ok && code == 1 {
			log.Printf("[HOLD-SWEEPER] released stuck reserve for %s", idempKey)
		}
	}
}

// idemp:{tenant}:{hash}. هش کولون ندارد، جدا کردن از آخرین کولون است.
func splitIdempKey(key string) (tenant, hash string) {
	rest, ok := strings.CutPrefix(key, "idemp:")
	if !ok {
		return "", ""
	}
	idx := strings.LastIndex(rest, ":")
	if idx <= 0 || idx == len(rest)-1 {
		return "", ""
	}
	return rest[:idx], rest[idx+1:]
}
