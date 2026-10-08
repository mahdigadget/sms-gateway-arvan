package cache

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestGenerateIdempotencyKey(t *testing.T) {
	first := GenerateIdempotencyKey("tenant", "09120000000", "1000", "hello")
	again := GenerateIdempotencyKey("tenant", "09120000000", "1000", "hello")
	if first != again {
		t.Fatalf("same message hashed differently: %s vs %s", first, again)
	}
	if len(first) != 32 {
		t.Fatalf("hash length = %d", len(first))
	}
	if GenerateIdempotencyKey("tenant", "09120000000", "1000", "hello!") == first {
		t.Fatal("different body produced the same hash")
	}
}

func TestMapReserveResult(t *testing.T) {
	stored, err := json.Marshal(map[string]any{
		"http_status": 202,
		"body":        map[string]string{"status": "queued"},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		in      any
		wantErr error
		replay  int
	}{
		{name: "reserved", in: int64(1)},
		{name: "in progress", in: int64(-1), wantErr: ErrRequestInProgress},
		{name: "cache cold", in: int64(-2), wantErr: ErrCacheNotReady},
		{name: "insufficient", in: int64(-3), wantErr: ErrInsufficientFund},
		{name: "unknown code", in: int64(9), wantErr: ErrUnknownLuaStatus},
		{name: "unknown type", in: 1, wantErr: ErrUnknownLuaStatus},
		{name: "legacy duplicate", in: "1", wantErr: ErrDuplicateRequest},
		{name: "stored replay", in: string(stored), replay: 202},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := mapReserveResult(test.in)
			if test.replay != 0 {
				var replay *ReplayResponse
				if !errors.As(err, &replay) || replay.HTTPStatus != test.replay {
					t.Fatalf("got %#v", err)
				}
				return
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("got %v, want %v", err, test.wantErr)
			}
		})
	}
}
