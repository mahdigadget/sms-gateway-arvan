package report

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	store *Store
	now   func() time.Time
}

func NewHandler(store *Store) *Handler {
	return &Handler{store: store, now: time.Now}
}

// Register مسیرهای گزارش را روی گروه داده شده می‌گذارد.
// GET /messages فهرست پیامک‌های tenant، GET /summary جمع همان بازه.
func Register(r gin.IRoutes, store *Store) {
	h := NewHandler(store)
	r.GET("/messages", h.Messages)
	r.GET("/summary", h.Summary)
}

func (h *Handler) Messages(c *gin.Context) {
	f, ok := h.bind(c)
	if !ok {
		return
	}
	page, err := h.store.List(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read sms report"})
		return
	}
	c.JSON(http.StatusOK, page)
}

func (h *Handler) Summary(c *gin.Context) {
	f, ok := h.bind(c)
	if !ok {
		return
	}
	summary, err := h.store.Summary(c.Request.Context(), f)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read sms summary"})
		return
	}
	c.JSON(http.StatusOK, summary)
}

func (h *Handler) bind(c *gin.Context) (Filter, bool) {
	f, err := ParseFilter(c.Request.URL.Query(), h.now())
	if err == nil {
		return f, true
	}
	switch {
	case errors.Is(err, ErrTenantRequired),
		errors.Is(err, ErrTenantInvalid),
		errors.Is(err, ErrTimeRange),
		errors.Is(err, ErrWindowTooWide),
		errors.Is(err, ErrKindInvalid),
		errors.Is(err, ErrStatusInvalid),
		errors.Is(err, ErrLimitInvalid),
		errors.Is(err, ErrOffsetInvalid),
		errors.Is(err, ErrTimeInvalid):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid report query"})
	}
	return Filter{}, false
}
