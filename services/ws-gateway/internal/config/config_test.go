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
