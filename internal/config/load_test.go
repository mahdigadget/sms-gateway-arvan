package config

import "testing"

func TestEnvOr(t *testing.T) {
	t.Setenv("SMS_TEST_PRESENT", "  otel:4317  ")
	if got := envOr("SMS_TEST_PRESENT", "fallback"); got != "otel:4317" {
		t.Fatalf("present = %q", got)
	}

	t.Setenv("SMS_TEST_BLANK", "   ")
	if got := envOr("SMS_TEST_BLANK", "fallback"); got != "fallback" {
		t.Fatalf("blank = %q", got)
	}
	if got := envOr("SMS_TEST_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("missing = %q", got)
	}
}

func TestTrimQuotes(t *testing.T) {
	tests := map[string]string{
		`"clickhouse"`: "clickhouse",
		"'secret'":     "secret",
		"plain":        "plain",
		`"mixed'`:      `"mixed'`,
		"a":            "a",
		"":             "",
	}
	for in, want := range tests {
		if got := trimQuotes(in); got != want {
			t.Errorf("trimQuotes(%q) = %q, want %q", in, got, want)
		}
	}
}
