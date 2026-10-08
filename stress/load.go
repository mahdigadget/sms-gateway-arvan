//go:build stress

package stress

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type kind int

const (
	kindVIP kind = iota
	kindNormal
)

type requestFn func(worker, seq int64) (method, url string, body []byte)

type loadConfig struct {
	Workers   int
	Duration  time.Duration
	VIPRatio  float64 // 0..1 در حالت mixed؛ برای VIP خالص ۱ و برای Normal خالص ۰
	Timeout   time.Duration
	MaxErrPct float64
	MaxP99    time.Duration
}

type loadStats struct {
	ok      atomic.Int64
	fail    atomic.Int64
	byCode  sync.Map // int -> *atomic.Int64
	latency []time.Duration
	mu      sync.Mutex
}

func (s *loadStats) addCode(code int) {
	v, _ := s.byCode.LoadOrStore(code, &atomic.Int64{})
	v.(*atomic.Int64).Add(1)
}

func (s *loadStats) addLatency(d time.Duration) {
	s.mu.Lock()
	s.latency = append(s.latency, d)
	s.mu.Unlock()
}

func (s *loadStats) snapshotCodes() map[int]int64 {
	out := map[int]int64{}
	s.byCode.Range(func(k, v any) bool {
		out[k.(int)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

func (s *loadStats) percentiles() (p50, p95, p99 time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.latency)
	if n == 0 {
		return 0, 0, 0
	}
	cp := append([]time.Duration(nil), s.latency...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[percentileIndex(n, 50)], cp[percentileIndex(n, 95)], cp[percentileIndex(n, 99)]
}

func percentileIndex(n, p int) int {
	if n == 1 {
		return 0
	}
	i := (p * n / 100) - 1
	if i < 0 {
		return 0
	}
	if i >= n {
		return n - 1
	}
	return i
}

func runLoad(cfg loadConfig, build requestFn) (*loadStats, time.Duration) {
	client := &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Workers * 2,
			MaxIdleConnsPerHost: cfg.Workers * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	stats := &loadStats{}
	var seq atomic.Int64
	stopAt := time.Now().Add(cfg.Duration)
	var wg sync.WaitGroup
	started := time.Now()

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for time.Now().Before(stopAt) {
				n := seq.Add(1)
				method, url, body := build(int64(worker), n)
				code, latency, err := doRequest(client, method, url, body)
				stats.addLatency(latency)
				if err != nil || code < 200 || code >= 300 {
					stats.fail.Add(1)
					if err != nil {
						stats.addCode(0)
					} else {
						stats.addCode(code)
					}
					continue
				}
				stats.ok.Add(1)
				stats.addCode(code)
			}
		}(w)
	}
	wg.Wait()
	return stats, time.Since(started)
}

func doRequest(client *http.Client, method, url string, body []byte) (int, time.Duration, error) {
	start := time.Now()
	req, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		return 0, time.Since(start), err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return 0, latency, err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, res.Body)
	return res.StatusCode, latency, nil
}

func vipBody(tenant string, worker, seq int64) []byte {
	payload := map[string]any{
		"tenant_id":   tenant,
		"destination": fmt.Sprintf("0912%07d", seq%10_000_000),
		"sender":      "1000",
		"body":        fmt.Sprintf("stress-vip-w%d-n%d-%d", worker, seq, time.Now().UnixNano()),
		"deadline_at": 30,
	}
	raw, _ := json.Marshal(payload)
	return raw
}

func normalBody(tenant string, worker, seq int64) []byte {
	payload := map[string]any{
		"tenant_id":   tenant,
		"destination": fmt.Sprintf("0913%07d", seq%10_000_000),
		"sender":      "2000",
		"body":        fmt.Sprintf("stress-normal-w%d-n%d-%d", worker, seq, time.Now().UnixNano()),
	}
	raw, _ := json.Marshal(payload)
	return raw
}

func report(stats *loadStats, elapsed time.Duration) string {
	ok := stats.ok.Load()
	fail := stats.fail.Load()
	total := ok + fail
	p50, p95, p99 := stats.percentiles()
	rps := float64(0)
	if elapsed > 0 {
		rps = float64(total) / elapsed.Seconds()
	}
	errPct := float64(0)
	if total > 0 {
		errPct = 100 * float64(fail) / float64(total)
	}
	return fmt.Sprintf(
		"total=%d ok=%d fail=%d err=%.2f%% rps=%.0f p50=%s p95=%s p99=%s codes=%v",
		total, ok, fail, errPct, rps, p50.Round(time.Millisecond), p95.Round(time.Millisecond), p99.Round(time.Millisecond), stats.snapshotCodes(),
	)
}

func errorRate(stats *loadStats) float64 {
	ok := stats.ok.Load()
	fail := stats.fail.Load()
	total := ok + fail
	if total == 0 {
		return 100
	}
	return 100 * float64(fail) / float64(total)
}
