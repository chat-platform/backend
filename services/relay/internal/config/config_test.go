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

		t.Run("maps outbox environment settings into outbox group", func(t *testing.T) {
			t.Setenv("RELAY_BATCH_SIZE", "25")
			t.Setenv("RELAY_CLAIM_LEASE", "45s")
			cfg, err := FromEnv(logger)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Outbox.BatchSize != 25 || cfg.Outbox.ClaimLease != 45*time.Second {
				t.Fatalf("Outbox=%+v, want batch size 25 and claim lease 45s", cfg.Outbox)
			}
		})
	}
}
