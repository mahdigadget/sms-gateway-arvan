package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestCheckReportsUnavailableWhenDepsMissing(t *testing.T) {
	c := New(nil, nil, nil)
	out := c.Check(context.Background())
	if out.Status != "unavailable" {
		t.Fatalf("status=%q", out.Status)
	}
	for _, name := range []string{"redis", "postgres", "kafka"} {
		if out.Checks[name].Status != "unavailable" {
			t.Fatalf("%s status=%q", name, out.Checks[name].Status)
		}
		if out.Checks[name].Error == "" {
			t.Fatalf("%s missing error detail", name)
		}
	}
}

func TestHandleReturns503WhenUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	Register(r, New(nil, nil, nil))

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
}
