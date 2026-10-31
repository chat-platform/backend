package config

import (
	"io"
	"testing"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

func TestMetricsAddressDefaultsAndOverride(t *testing.T) {
	t.Setenv("METRICS_ADDR", "")
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if got := FromEnv(logger).MetricsAddr; got != "127.0.0.1:9094" {
		t.Fatalf("default MetricsAddr=%q, want 127.0.0.1:9094", got)
	}

	t.Setenv("METRICS_ADDR", "127.0.0.1:19094")
	if got := FromEnv(logger).MetricsAddr; got != "127.0.0.1:19094" {
		t.Fatalf("overridden MetricsAddr=%q, want 127.0.0.1:19094", got)
	}
}

func TestAllowedOriginsDefaultAndOverride(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CORS_ALLOWED_ORIGINS", "")
	if got := FromEnv(logger).AllowedOrigins; len(got) != 2 || got[0] != "http://localhost:5173" || got[1] != "https://messenger-as4.pages.dev" {
		t.Fatalf("default AllowedOrigins=%q", got)
	}

	t.Setenv("CORS_ALLOWED_ORIGINS", "https://one.example, https://two.example ")
	got := FromEnv(logger).AllowedOrigins
	if len(got) != 2 || got[0] != "https://one.example" || got[1] != "https://two.example" {
		t.Fatalf("overridden AllowedOrigins=%q", got)
	}
}
