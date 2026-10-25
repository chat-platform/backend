package config

import (
	"io"
	"testing"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

func TestMetricsAddressDefaultsAndCanBeOverridden(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GRPC_ADDR", ":50051")
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("REDIS_ADDR", "localhost:6379")
	t.Setenv("METRICS_ADDR", "")
	if got := FromEnv(logger).MetricsAddr; got != "127.0.0.1:9092" {
		t.Fatalf("default metrics address=%q, want loopback default", got)
	}
	t.Setenv("METRICS_ADDR", "127.0.0.1:9192")
	if got := FromEnv(logger).MetricsAddr; got != "127.0.0.1:9192" {
		t.Fatalf("overridden metrics address=%q, want configured address", got)
	}
}
