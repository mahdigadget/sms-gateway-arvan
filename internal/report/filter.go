package report

import (
	"errors"
	"net/url"
	"time"

	"github.com/google/uuid"
)

const (
	defaultWindow = 7 * 24 * time.Hour
	maxWindow     = 93 * 24 * time.Hour
	defaultLimit  = 50
	maxLimit      = 200
	maxOffset     = 100_000
)

var (
	ErrTenantRequired = errors.New("tenant_id is required")
	ErrTenantInvalid  = errors.New("tenant_id must be a uuid")
	ErrTimeRange      = errors.New("from must be before to")
	ErrWindowTooWide  = errors.New("time window is wider than 93 days")
	ErrKindInvalid    = errors.New("kind must be otp or normal")
	ErrStatusInvalid  = errors.New("status must be accepted, rejected, or refunded")
	ErrLimitInvalid   = errors.New("limit must be a positive integer")
	ErrOffsetInvalid  = errors.New("offset must be a non-negative integer")
	ErrTimeInvalid    = errors.New("from and to must be RFC3339 or YYYY-MM-DD")
)

// Filter بازه و فیلتر گزارش یک tenant. tenant خالی قبول نمی‌شود تا گزارش بقیه دیده نشود.
type Filter struct {
	TenantID string
	From     time.Time
	To       time.Time
	Kind     string
	Status   string
	Limit    int
	Offset   int
}

func (f Filter) eventType() string {
	if f.Status == "" {
		return ""
	}
	return "message." + f.Status
}

// ParseFilter پارامترهای کوئری گزارش را می‌خواند.
// بدون from/to، هفت روز اخیر تا همین لحظه. تاریخ روز به‌تنهایی نیمه‌شب UTC است.
func ParseFilter(q url.Values, now time.Time) (Filter, error) {
	now = now.UTC()
	f := Filter{
		TenantID: q.Get("tenant_id"),
		Kind:     q.Get("kind"),
		Status:   q.Get("status"),
		Limit:    defaultLimit,
	}
	if f.TenantID == "" {
		return Filter{}, ErrTenantRequired
	}
	if _, err := uuid.Parse(f.TenantID); err != nil {
		return Filter{}, ErrTenantInvalid
	}
	if f.Kind != "" && f.Kind != "otp" && f.Kind != "normal" {
		return Filter{}, ErrKindInvalid
	}
	switch f.Status {
	case "", "accepted", "rejected", "refunded":
	default:
		return Filter{}, ErrStatusInvalid
	}

	from, hasFrom, err := parseTime(q.Get("from"))
	if err != nil {
		return Filter{}, err
	}
	to, hasTo, err := parseTime(q.Get("to"))
	if err != nil {
		return Filter{}, err
	}
	switch {
	case hasFrom && hasTo:
		f.From, f.To = from, to
	case hasFrom:
		f.From, f.To = from, now
	case hasTo:
		f.From, f.To = to.Add(-defaultWindow), to
	default:
		f.From, f.To = now.Add(-defaultWindow), now
	}
	if !f.From.Before(f.To) {
		return Filter{}, ErrTimeRange
	}
	if f.To.Sub(f.From) > maxWindow {
		return Filter{}, ErrWindowTooWide
	}

	if raw := q.Get("limit"); raw != "" {
		n, err := parseNonNeg(raw)
		if err != nil || n == 0 || n > maxLimit {
			return Filter{}, ErrLimitInvalid
		}
		f.Limit = n
	}
	if raw := q.Get("offset"); raw != "" {
		n, err := parseNonNeg(raw)
		if err != nil || n > maxOffset {
			return Filter{}, ErrOffsetInvalid
		}
		f.Offset = n
	}
	return f, nil
}

func parseTime(raw string) (time.Time, bool, error) {
	if raw == "" {
		return time.Time{}, false, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), true, nil
	}
	if t, err := time.Parse("2006-01-02", raw); err == nil {
		return t.UTC(), true, nil
	}
	return time.Time{}, false, ErrTimeInvalid
}

func parseNonNeg(raw string) (int, error) {
	n := 0
	for _, c := range raw {
		if c < '0' || c > '9' {
			return 0, errors.New("not a number")
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}
