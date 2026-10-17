package main

import (
	"context"
	"fmt"
	postgresadapter "github.com/chat-platform/backend/services/relay/internal/adapter/outbound/postgres"
	redisadapter "github.com/chat-platform/backend/services/relay/internal/adapter/outbound/redis"
	"github.com/chat-platform/backend/services/relay/internal/app"
	"github.com/chat-platform/backend/services/relay/internal/config"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, err := logging.New(os.Stdout, logging.Config{Level: os.Getenv("LOG_LEVEL"), Format: os.Getenv("LOG_FORMAT"), AddSource: os.Getenv("LOG_ADD_SOURCE") == "true"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger = logger.With("service", "relay")
	cfg := config.FromEnv(logger)
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		fatal(ctx, logger, "connect to postgres", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fatal(ctx, logger, "ping postgres", err)
	}
	rc := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rc.Close()
	if err := rc.Ping(ctx).Err(); err != nil {
		fatal(ctx, logger, "ping redis", err)
	}
	dispatcher := app.New(postgresadapter.NewOutboxRepository(pool), redisadapter.NewRegistry(rc), redisadapter.NewPublisher(rc), cfg.BatchSize, logger.With("component", "dispatcher"))
	logger.Info(ctx, "relay started", "batch_size", cfg.BatchSize, "poll_interval", cfg.PollInterval.String())
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Info(context.Background(), "relay stopping")
			return
		case <-ticker.C:
			count, err := dispatcher.RunOnce(ctx)
			if err != nil {
				logger.Error(ctx, "relay drain iteration failed", "error", err)
				continue
			}
			if count == 0 {
				logger.Debug(ctx, "relay found no undispatched rows")
			}
		}
	}
}
func fatal(ctx context.Context, l logging.Logger, message string, err error) {
	l.Error(ctx, message, "error", err)
	os.Exit(1)
}
