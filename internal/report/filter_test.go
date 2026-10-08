package report

import (
	"errors"
	"net/url"
	"testing"
	"time"
)

func TestParseFilterDefaults(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	q := url.Values{"tenant_id": {"11111111-1111-1111-1111-111111111111"}}
	f, err := ParseFilter(q, now)
	if err != nil {
		t.Fatal(err)
	}
	if !f.From.Equal(now.Add(-defaultWindow)) || !f.To.Equal(now) {
		t.Fatalf("window = %s .. %s", f.From, f.To)
	}
	if f.Limit != defaultLimit || f.Offset != 0 {
		t.Fatalf("page = %d/%d", f.Limit, f.Offset)
	}
}

func TestParseFilterRejectsOtherTenantsShape(t *testing.T) {
	now := time.Now().UTC()
	_, err := ParseFilter(url.Values{}, now)
	if !errors.Is(err, ErrTenantRequired) {
		t.Fatalf("got %v", err)
	}
	_, err = ParseFilter(url.Values{"tenant_id": {"not-a-uuid"}}, now)
	if !errors.Is(err, ErrTenantInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestParseFilterBounds(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	q := url.Values{
		"tenant_id": {"11111111-1111-1111-1111-111111111111"},
		"from":      {"2026-01-01"},
		"to":        {"2026-10-01"},
		"kind":      {"otp"},
		"status":    {"accepted"},
		"limit":     {"10"},
		"offset":    {"20"},
	}
	_, err := ParseFilter(q, now)
	if !errors.Is(err, ErrWindowTooWide) {
		t.Fatalf("wide window: %v", err)
	}

	q.Set("from", "2026-09-01")
	f, err := ParseFilter(q, now)
	if err != nil {
		t.Fatal(err)
	}
	if f.Kind != "otp" || f.Status != "accepted" || f.Limit != 10 || f.Offset != 20 {
		t.Fatalf("%+v", f)
	}
	if f.eventType() != "message.accepted" {
		t.Fatal(f.eventType())
	}
}

func TestParseFilterRejectsBadQuery(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	tenant := url.Values{"tenant_id": {"11111111-1111-1111-1111-111111111111"}}

	tests := []struct {
		name string
		set  func(url.Values)
		want error
	}{
		{name: "kind", set: func(q url.Values) { q.Set("kind", "vip") }, want: ErrKindInvalid},
		{name: "status", set: func(q url.Values) { q.Set("status", "sent") }, want: ErrStatusInvalid},
		{name: "time", set: func(q url.Values) { q.Set("from", "yesterday") }, want: ErrTimeInvalid},
		{name: "range", set: func(q url.Values) {
			q.Set("from", "2026-10-02")
			q.Set("to", "2026-10-01")
		}, want: ErrTimeRange},
		{name: "limit", set: func(q url.Values) { q.Set("limit", "0") }, want: ErrLimitInvalid},
		{name: "offset", set: func(q url.Values) { q.Set("offset", "-1") }, want: ErrOffsetInvalid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := url.Values{}
			for key, values := range tenant {
				q[key] = values
			}
			test.set(q)
			_, err := ParseFilter(q, now)
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v", err)
			}
		})
	}
}
