package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"sms-gateway-arvan/internal/cache"

	"github.com/gin-gonic/gin"
)

func TestHandleReserveError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stored, err := json.Marshal(map[string]string{"status": "queued"})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		err    error
		status int
		want   string
	}{
		{name: "replay", err: &cache.ReplayResponse{HTTPStatus: http.StatusAccepted, Body: stored}, status: http.StatusAccepted, want: "queued"},
		{name: "broken replay", err: &cache.ReplayResponse{HTTPStatus: http.StatusAccepted, Body: []byte("{")}, status: http.StatusConflict, want: "duplicate request detected"},
		{name: "in progress", err: cache.ErrRequestInProgress, status: http.StatusConflict, want: "request in progress"},
		{name: "duplicate", err: cache.ErrDuplicateRequest, status: http.StatusConflict, want: "duplicate request detected"},
		{name: "cache cold", err: cache.ErrCacheNotReady, status: http.StatusServiceUnavailable, want: "account cache warming up"},
		{name: "insufficient", err: cache.ErrInsufficientFund, status: http.StatusPaymentRequired, want: "insufficient wallet balance"},
		{name: "other", err: errors.New("redis down"), status: http.StatusInternalServerError, want: "balance reservation failed"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			handleReserveError(ctx, test.err)
			if recorder.Code != test.status || !strings.Contains(recorder.Body.String(), test.want) {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}
