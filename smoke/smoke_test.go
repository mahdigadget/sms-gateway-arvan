//go:build smoke

// اسموک‌تست روی استک واقعی (مثلاً docker compose).
// پیش‌نیاز: سرویس‌ها بالا باشند و ./mock زده شده باشد.
//
//	docker compose up -d --build
//	POSTGRES_DSN='postgres://postgres:1234@127.0.0.1:5433/sms_gateway?sslmode=disable' \
//	  REDIS_PORT=6380 ./mock
//	go test -tags smoke -count=1 -timeout 2m ./smoke
package smoke

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// همان کاربر اسکریپت ./mock و محیط Bruno.
const mockTenant = "11111111-1111-1111-1111-111111111111"

var (
	apiURL    string
	reportURL string
	client    = &http.Client{Timeout: 8 * time.Second}
)

func TestMain(m *testing.M) {
	apiURL = env("SMOKE_API_URL", "http://127.0.0.1:9095")
	reportURL = env("SMOKE_REPORT_URL", "http://127.0.0.1:9096")

	if err := pingHost(apiURL); err != nil {
		fmt.Fprintf(os.Stderr, "smoke: api unreachable (%s): %v\n", apiURL, err)
		fmt.Fprintln(os.Stderr, "smoke: first run: docker compose up -d --build && ./mock (با پورت‌های داکر)")
		os.Exit(1)
	}
	if err := pingHost(reportURL); err != nil {
		fmt.Fprintf(os.Stderr, "smoke: report unreachable (%s): %v\n", reportURL, err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestSendVIP_Accepted(t *testing.T) {
	body := fmt.Sprintf("smoke vip %d", time.Now().UnixNano())
	res := postJSON(t, apiURL+"/api/v1/sms/vip", map[string]any{
		"tenant_id":   mockTenant,
		"destination": "09120000001",
		"sender":      "1000",
		"body":        body,
		"deadline_at": 30,
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("VIP status=%d body=%s", res.StatusCode, readBody(t, res))
	}
	got := decodeMap(t, res)
	if got["status"] != "queued" || got["type"] != "vip" {
		t.Fatalf("unexpected VIP payload: %v", got)
	}
	if got["idemp"] == "" {
		t.Fatal("missing idemp")
	}
}

func TestSendNormal_Accepted(t *testing.T) {
	body := fmt.Sprintf("smoke normal %d", time.Now().UnixNano())
	res := postJSON(t, apiURL+"/api/v1/sms/normal", map[string]any{
		"tenant_id":   mockTenant,
		"destination": "09120000002",
		"sender":      "2000",
		"body":        body,
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("normal status=%d body=%s", res.StatusCode, readBody(t, res))
	}
	got := decodeMap(t, res)
	if got["status"] != "queued" || got["type"] != "normal" {
		t.Fatalf("unexpected normal payload: %v", got)
	}
}

func TestSendVIP_ReplaySameBody(t *testing.T) {
	payload := map[string]any{
		"tenant_id":   mockTenant,
		"destination": "09120000003",
		"sender":      "1000",
		"body":        fmt.Sprintf("smoke replay %d", time.Now().UnixNano()),
		"deadline_at": 30,
	}

	first := postJSON(t, apiURL+"/api/v1/sms/vip", payload)
	defer first.Body.Close()
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first VIP status=%d body=%s", first.StatusCode, readBody(t, first))
	}
	idemp := decodeMap(t, first)["idemp"]

	second := postJSON(t, apiURL+"/api/v1/sms/vip", payload)
	defer second.Body.Close()
	if second.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status=%d body=%s", second.StatusCode, readBody(t, second))
	}
	replay := decodeMap(t, second)
	if replay["idemp"] != idemp {
		t.Fatalf("replay idemp changed: first=%v second=%v", idemp, replay["idemp"])
	}
}

func TestSendVIP_UnknownTenantWarmingUp(t *testing.T) {
	res := postJSON(t, apiURL+"/api/v1/sms/vip", map[string]any{
		"tenant_id":   "00000000-0000-0000-0000-000000000099",
		"destination": "09120000099",
		"sender":      "1000",
		"body":        "smoke missing wallet",
		"deadline_at": 30,
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("want 503 for missing wallet, got %d body=%s", res.StatusCode, readBody(t, res))
	}
}

func TestSendVIP_ValidationRejectsBadDeadline(t *testing.T) {
	res := postJSON(t, apiURL+"/api/v1/sms/vip", map[string]any{
		"tenant_id":   mockTenant,
		"destination": "09120000004",
		"sender":      "1000",
		"body":        "smoke bad deadline",
		"deadline_at": 0,
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for deadline_at=0, got %d body=%s", res.StatusCode, readBody(t, res))
	}
}

func TestReportMessages_OK(t *testing.T) {
	url := reportURL + "/api/v1/sms/report/messages?tenant_id=" + mockTenant + "&limit=50&offset=0"
	res := get(t, url)
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("messages status=%d body=%s", res.StatusCode, readBody(t, res))
	}
	var page struct {
		TenantID string `json:"tenant_id"`
		Total    uint64 `json:"total"`
		Items    []any  `json:"items"`
	}
	decodeInto(t, res, &page)
	if page.TenantID != mockTenant {
		t.Fatalf("tenant_id=%q", page.TenantID)
	}
	if page.Items == nil {
		t.Fatal("items must be an array")
	}
	// بعد از ./mock باید حداقل چند ردیف باشد؛ اگر خالی بود فقط هشدار نمی‌دهیم چون ممکن است فقط ارسال تازه باشد.
	t.Logf("messages total=%d items=%d", page.Total, len(page.Items))
}

func TestReportSummary_OK(t *testing.T) {
	url := reportURL + "/api/v1/sms/report/summary?tenant_id=" + mockTenant
	res := get(t, url)
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("summary status=%d body=%s", res.StatusCode, readBody(t, res))
	}
	var summary struct {
		TenantID string `json:"tenant_id"`
		Total    uint64 `json:"total"`
		NetCost  int64  `json:"net_cost"`
	}
	decodeInto(t, res, &summary)
	if summary.TenantID != mockTenant {
		t.Fatalf("tenant_id=%q", summary.TenantID)
	}
	t.Logf("summary total=%d net_cost=%d", summary.Total, summary.NetCost)
}

func TestReport_RejectsMissingTenant(t *testing.T) {
	res := get(t, reportURL+"/api/v1/sms/report/summary")
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 without tenant_id, got %d body=%s", res.StatusCode, readBody(t, res))
	}
}

func TestReport_RejectsBadKind(t *testing.T) {
	url := reportURL + "/api/v1/sms/report/messages?tenant_id=" + mockTenant + "&kind=vip"
	res := get(t, url)
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400 for kind=vip, got %d body=%s", res.StatusCode, readBody(t, res))
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func pingHost(base string) error {
	req, err := http.NewRequest(http.MethodGet, base, nil)
	if err != nil {
		return err
	}
	// خود مسیر ریشه ممکن است 404 بدهد؛ فقط وصل شدن TCP/HTTP مهم است.
	res, err := client.Do(req)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return err
		}
		return err
	}
	_ = res.Body.Close()
	return nil
}

func postJSON(t *testing.T, url string, payload map[string]any) *http.Response {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return res
}

func get(t *testing.T, url string) *http.Response {
	t.Helper()
	res, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return res
}

func readBody(t *testing.T, res *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func decodeMap(t *testing.T, res *http.Response) map[string]any {
	t.Helper()
	var got map[string]any
	decodeInto(t, res, &got)
	return got
}

func decodeInto(t *testing.T, res *http.Response, dst any) {
	t.Helper()
	if err := json.NewDecoder(res.Body).Decode(dst); err != nil {
		t.Fatal(err)
	}
}
