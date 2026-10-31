package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	postgresadapter "github.com/chat-platform/backend/services/relay/internal/adapter/outbound/postgres"
	redisadapter "github.com/chat-platform/backend/services/relay/internal/adapter/outbound/redis"
	"github.com/chat-platform/backend/services/relay/internal/app"
	"github.com/chat-platform/backend/services/relay/internal/config"
	relaymetrics "github.com/chat-platform/backend/services/relay/internal/metrics"
	"github.com/chat-platform/backend/shared/pkg/envfile"
	"github.com/chat-platform/backend/shared/pkg/logging"
	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := envfile.Load(); err != nil {
		fmt.Fprintln(os.Stderr, "load environment file:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, err := logging.New(os.Stdout, logging.Config{Level: os.Getenv("LOG_LEVEL"), Format: os.Getenv("LOG_FORMAT"), AddSource: os.Getenv("LOG_ADD_SOURCE") == "true"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger = logger.With("service", "relay")
	cfg, err := config.FromEnv(logger)
	if err != nil {
		fatal(ctx, logger, "invalid relay configuration", err)
	}
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
	outboxRepository := postgresadapter.NewOutboxRepository(pool, cfg.Outbox.SnapshotCountCap)
	registry, err := sharedmetrics.NewRegistry()
	if err != nil {
		fatal(ctx, logger, "create metrics registry", err)
	}
	outboxMetrics, err := relaymetrics.NewOutboxMetrics(registry)
	if err != nil {
		fatal(ctx, logger, "register outbox metrics", err)
	}
	dispatchMetrics, err := relaymetrics.NewDispatchMetrics(registry)
	if err != nil {
		fatal(ctx, logger, "register dispatch metrics", err)
	}
	metricsServer, err := sharedmetrics.Start(cfg.MetricsAddr, registry)
	if err != nil {
		logger.Error(ctx, "metrics listener failed to start", "address", cfg.MetricsAddr, "error", err)
		os.Exit(1)
	}
	go func() {
		if err := <-metricsServer.Done(); ctx.Err() == nil {
			if err == nil {
				err = errors.New("metrics listener stopped unexpectedly")
			}
			logger.Error(context.Background(), "metrics server failed", "address", cfg.MetricsAddr, "error", err)
			stop()
		}
	}()
	dispatcher := app.New(outboxRepository, redisadapter.NewRegistry(rc), redisadapter.NewPublisher(rc), app.OutboxOptions{
		BatchSize: cfg.Outbox.BatchSize, RetryBatchSize: cfg.Retry.BatchSize, ClaimLease: cfg.Outbox.ClaimLease,
	}, logger.With("component", "dispatcher"), dispatchMetrics)
	logger.Info(ctx, "relay started", "workers", cfg.Workers.Count, "batch_size", cfg.Outbox.BatchSize, "poll_interval", cfg.Workers.PollInterval.String(), "claim_lease", cfg.Outbox.ClaimLease.String(), "metrics_address", cfg.MetricsAddr)
	var workers sync.WaitGroup
	workers.Add(cfg.Workers.Count + 3)
	go func() {
		defer workers.Done()
		runCleanupWorker(ctx, outboxRepository, logger.With("component", "outbox_cleanup"))
	}()
	go func() {
		defer workers.Done()
		runRetryWorker(ctx, cfg.Retry.Interval, dispatcher, logger.With("component", "retry"))
	}()
	go func() {
		defer workers.Done()
		relaymetrics.RunOutboxSampler(ctx, relaymetrics.DefaultSnapshotInterval, relaymetrics.DefaultSnapshotTimeout, outboxRepository, outboxMetrics, logger.With("component", "outbox_metrics"))
	}()
	for workerID := 1; workerID <= cfg.Workers.Count; workerID++ {
		go func(id int) {
			defer workers.Done()
			runWorker(ctx, id, cfg.Workers.PollInterval, dispatcher, logger)
		}(workerID)
	}
	<-ctx.Done()
	logger.Info(context.Background(), "relay stopping", "workers", cfg.Workers.Count, "metrics_address", cfg.MetricsAddr)
	workers.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error(context.Background(), "metrics server shutdown failed", "address", cfg.MetricsAddr, "error", err)
	}
	cancel()
	logger.Info(context.Background(), "relay stopped")
}

type retryDispatcher interface {
	RetryOnce(context.Context) (int, error)
}

func runRetryWorker(ctx context.Context, interval time.Duration, dispatcher retryDispatcher, logger logging.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	logger.Info(ctx, "relay retry worker started", "interval", interval.String())
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			count, err := dispatcher.RetryOnce(ctx)
			if err != nil {
				logger.Error(ctx, "relay retry iteration failed", "error", err)
				continue
			}
			if count > 0 {
				logger.Info(ctx, "relay retry iteration completed", "rows_retried", count)
			}
		}
	}
}

type outboxCleaner interface {
	DeleteExpiredBatch(context.Context, int) (int64, error)
}

func runCleanupWorker(ctx context.Context, cleaner outboxCleaner, logger logging.Logger) {
	const batchSize = 1000
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		var deleted int64
		for ctx.Err() == nil {
			count, err := cleaner.DeleteExpiredBatch(ctx, batchSize)
			if err != nil {
				logger.Error(ctx, "direct outbox cleanup failed", "deleted_rows", deleted, "error", err)
				break
			}
			deleted += count
			if count < batchSize {
				break
			}
		}
		if deleted > 0 {
			logger.Info(ctx, "direct outbox cleanup completed", "deleted_rows", deleted)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type workerDispatcher interface {
	RunOnce(context.Context) (int, error)
}

func runWorker(ctx context.Context, workerID int, pollInterval time.Duration, dispatcher workerDispatcher, logger logging.Logger) {
	workerLogger := logger.With("worker_id", workerID)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	workerLogger.Info(ctx, "relay worker started", "poll_interval", pollInterval.String())
	defer func() {
		workerLogger.Info(context.Background(), "relay worker stopped")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := dispatcher.RunOnce(ctx)
			if err != nil {
				workerLogger.Error(ctx, "relay drain iteration failed", "error", err)
				continue
			}
		}
	}
}

func fatal(ctx context.Context, l logging.Logger, message string, err error) {
	l.Error(ctx, message, "error", err)
	os.Exit(1)
}
