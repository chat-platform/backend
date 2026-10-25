package config

import (
	"io"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

func TestFromEnvRelayWorkers(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("defaults to two when unset", func(t *testing.T) {
		t.Setenv("RELAY_WORKERS", "")
		t.Setenv("RELAY_POLL_INTERVAL", "")
		t.Setenv("RELAY_BATCH_SIZE", "")
		t.Setenv("RELAY_CLAIM_LEASE", "")
		t.Setenv("RELAY_RETRY_INTERVAL", "")
		t.Setenv("RELAY_RETRY_BATCH_SIZE", "")
		t.Setenv("METRICS_ADDR", "")
		cfg, err := FromEnv(logger)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Workers.Count != 2 {
			t.Fatalf("Workers.Count=%d, want 2", cfg.Workers.Count)
		}
		if cfg.Workers.PollInterval != 250*time.Millisecond {
			t.Fatalf("Workers.PollInterval=%s, want 250ms", cfg.Workers.PollInterval)
		}
		if cfg.Outbox.BatchSize != 100 {
			t.Fatalf("Outbox.BatchSize=%d, want 100", cfg.Outbox.BatchSize)
		}
		if cfg.Outbox.ClaimLease != 30*time.Second {
			t.Fatalf("Outbox.ClaimLease=%s, want 30s", cfg.Outbox.ClaimLease)
		}
		if cfg.Retry.Interval != 10*time.Second || cfg.Retry.BatchSize != 100 {
			t.Fatalf("Retry=%+v, want 10s and 100", cfg.Retry)
		}
		if cfg.MetricsAddr != "127.0.0.1:9093" {
			t.Fatalf("MetricsAddr=%q, want 127.0.0.1:9093", cfg.MetricsAddr)
		}
	})

	t.Run("accepts a positive integer", func(t *testing.T) {
		t.Setenv("RELAY_WORKERS", "5")
		cfg, err := FromEnv(logger)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Workers.Count != 5 {
			t.Fatalf("Workers.Count=%d, want 5", cfg.Workers.Count)
		}
	})

	for _, value := range []string{"0", "-1", "abc", "1.5"} {
		t.Run("rejects "+value, func(t *testing.T) {
			t.Setenv("RELAY_WORKERS", value)
			if _, err := FromEnv(logger); err == nil {
				t.Fatalf("FromEnv() accepted RELAY_WORKERS=%q", value)
			}
		})
	}

	t.Run("maps outbox environment settings into outbox group", func(t *testing.T) {
		t.Setenv("RELAY_WORKERS", "2")
		t.Setenv("RELAY_BATCH_SIZE", "25")
		t.Setenv("RELAY_CLAIM_LEASE", "45s")
		t.Setenv("RELAY_RETRY_INTERVAL", "15s")
		t.Setenv("RELAY_RETRY_BATCH_SIZE", "35")
		cfg, err := FromEnv(logger)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Outbox.BatchSize != 25 || cfg.Outbox.ClaimLease != 45*time.Second {
			t.Fatalf("Outbox=%+v, want batch size 25 and claim lease 45s", cfg.Outbox)
		}
		if cfg.Retry.Interval != 15*time.Second || cfg.Retry.BatchSize != 35 {
			t.Fatalf("Retry=%+v, want 15s and 35", cfg.Retry)
		}
	})

	t.Run("accepts metrics address override", func(t *testing.T) {
		t.Setenv("METRICS_ADDR", "127.0.0.1:19093")
		cfg, err := FromEnv(logger)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MetricsAddr != "127.0.0.1:19093" {
			t.Fatalf("MetricsAddr=%q, want 127.0.0.1:19093", cfg.MetricsAddr)
		}
	})
}
