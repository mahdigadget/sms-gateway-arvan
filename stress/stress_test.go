//go:build stress

// استرس‌تست روی استک واقعی (api باید بالا باشد).
// هر درخواست بدنهٔ یکتا دارد تا به replay ایدمپتانسی نخورید.
//
// پیش‌نیاز:
//
//	docker compose up -d --build
//	تاپیک‌های کافکا ساخته شده باشند
//	کیف‌پول tenant به اندازهٔ کافی موجودی داشته باشد (هر ارسال موفق cost=1)
//
// مثال:
//
//	STRESS_REDIS_ADDR=127.0.0.1:6380 STRESS_WALLET=200000 \
//	  STRESS_WORKERS=32 STRESS_DURATION=30s \
//	  go test -tags stress -count=1 -timeout 10m ./stress
//
// از داخل شبکهٔ compose (اگر پورت هاست گیر دارد):
//
//	STRESS_API_URL=http://api:9095 STRESS_REDIS_ADDR=redis:6379 ...
package stress

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultTenant = "11111111-1111-1111-1111-111111111111"

var (
	apiURL string
	tenant string
	cfg    loadConfig
)

func TestMain(m *testing.M) {
	apiURL = env("STRESS_API_URL", "http://127.0.0.1:9095")
	tenant = env("STRESS_TENANT", defaultTenant)
	cfg = loadConfig{
		Workers:   envInt("STRESS_WORKERS", 32),
		Duration:  envDuration("STRESS_DURATION", 30*time.Second),
		VIPRatio:  envFloat("STRESS_VIP_RATIO", 0.3),
		Timeout:   envDuration("STRESS_TIMEOUT", 5*time.Second),
		MaxErrPct: envFloat("STRESS_MAX_ERR_PCT", 1),
		MaxP99:    envDuration("STRESS_MAX_P99", 2*time.Second),
	}

	if err := pingAPI(apiURL); err != nil {
		fmt.Fprintf(os.Stderr, "stress: api unreachable (%s): %v\n", apiURL, err)
		fmt.Fprintln(os.Stderr, "stress: استک را بالا بیاور؛ در صورت نیاز STRESS_API_URL را روی http://api:9095 بگذار")
		os.Exit(1)
	}
	if addr := os.Getenv("STRESS_REDIS_ADDR"); addr != "" {
		wallet := envInt("STRESS_WALLET", 200_000)
		if err := seedWallet(addr, tenant, wallet); err != nil {
			fmt.Fprintf(os.Stderr, "stress: seed redis wallet failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "stress: wallet %s available=%d on %s\n", tenant, wallet, addr)
	}

	os.Exit(m.Run())
}

func TestStressVIP(t *testing.T) {
	runAndAssert(t, "vip", loadConfig{
		Workers:   cfg.Workers,
		Duration:  cfg.Duration,
		Timeout:   cfg.Timeout,
		MaxErrPct: cfg.MaxErrPct,
		MaxP99:    cfg.MaxP99,
		VIPRatio:  1,
	}, func(worker, seq int64) (string, string, []byte) {
		return http.MethodPost, apiURL + "/api/v1/sms/vip", vipBody(tenant, worker, seq)
	})
}

func TestStressNormal(t *testing.T) {
	runAndAssert(t, "normal", loadConfig{
		Workers:   cfg.Workers,
		Duration:  cfg.Duration,
		Timeout:   cfg.Timeout,
		MaxErrPct: cfg.MaxErrPct,
		MaxP99:    cfg.MaxP99,
		VIPRatio:  0,
	}, func(worker, seq int64) (string, string, []byte) {
		return http.MethodPost, apiURL + "/api/v1/sms/normal", normalBody(tenant, worker, seq)
	})
}

func TestStressMixed(t *testing.T) {
	ratio := cfg.VIPRatio
	runAndAssert(t, "mixed", cfg, func(worker, seq int64) (string, string, []byte) {
		// تقریبی ratio با باقی‌ماندهٔ seq
		if float64(seq%100)/100.0 < ratio {
			return http.MethodPost, apiURL + "/api/v1/sms/vip", vipBody(tenant, worker, seq)
		}
		return http.MethodPost, apiURL + "/api/v1/sms/normal", normalBody(tenant, worker, seq)
	})
}

func runAndAssert(t *testing.T, name string, c loadConfig, build requestFn) {
	t.Helper()
	t.Logf("stress %s workers=%d duration=%s", name, c.Workers, c.Duration)

	stats, elapsed := runLoad(c, build)
	summary := report(stats, elapsed)
	t.Log(summary)
	fmt.Fprintf(os.Stderr, "stress[%s]: %s\n", name, summary)

	if stats.ok.Load()+stats.fail.Load() == 0 {
		t.Fatal("no requests completed")
	}
	if rate := errorRate(stats); rate > c.MaxErrPct {
		t.Fatalf("error rate %.2f%% exceeds max %.2f%%", rate, c.MaxErrPct)
	}
	_, _, p99 := stats.percentiles()
	if c.MaxP99 > 0 && p99 > c.MaxP99 {
		t.Fatalf("p99 %s exceeds max %s", p99, c.MaxP99)
	}
}

func pingAPI(base string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get(base + "/health")
	if err != nil {
		// اگر /health هنوز روی نسخهٔ قدیمی نبود، خود ریشه کافی است برای «وصل بودن»
		res, err = client.Get(base)
		if err != nil {
			return err
		}
	}
	defer res.Body.Close()
	return nil
}

func seedWallet(addr, tenantID string, available int) error {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}
	return rdb.HSet(ctx, "wallet:"+tenantID, "available", available, "reserved", 0).Err()
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
