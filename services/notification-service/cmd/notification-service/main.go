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

	redisadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/inbound/redis"
	fcmadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/outbound/fcm"
	postgresadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/outbound/postgres"
	"github.com/chat-platform/backend/services/notification-service/internal/app"
	"github.com/chat-platform/backend/services/notification-service/internal/config"
	servicemetrics "github.com/chat-platform/backend/services/notification-service/internal/metrics"
	"github.com/chat-platform/backend/shared/pkg/logging"
	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, err := logging.New(os.Stdout, logging.Config{Level: loggerEnv("LOG_LEVEL", "info"), Format: loggerEnv("LOG_FORMAT", "json"), AddSource: os.Getenv("LOG_ADD_SOURCE") == "true"})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger = logger.With("service", "notification-service")
	cfg, err := config.FromEnv()
	if err != nil {
		logger.Error(ctx, "load configuration", "error", err)
		os.Exit(1)
	}
	fcmClient, err := fcmadapter.NewADCClient(ctx, cfg.GoogleCloudProject)
	if err != nil {
		logger.Error(ctx, "initialize FCM client", "error", err)
		os.Exit(1)
	}
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error(ctx, "connect to postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		logger.Error(ctx, "ping postgres", "error", err)
		os.Exit(1)
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		logger.Error(ctx, "ping redis", "error", err)
		os.Exit(1)
	}
	registry, err := sharedmetrics.NewRegistry()
	if err != nil {
		logger.Error(ctx, "create metrics registry", "error", err)
		os.Exit(1)
	}
	serviceMetrics, err := servicemetrics.New(registry)
	if err != nil {
		logger.Error(ctx, "register Notification Service metrics", "error", err)
		os.Exit(1)
	}
	host, _ := os.Hostname()
	consumerID := fmt.Sprintf("%s-%d", host, os.Getpid())
	notifier := app.New(postgresadapter.NewTokenRepository(pool), fcmadapter.NewSender(fcmClient, serviceMetrics), logger.With("component", "notifier"), serviceMetrics)
	consumer := redisadapter.NewConsumer(client, consumerID, notifier, logger.With("component", "stream_consumer"), cfg.NotificationCoalesceWindow)
	if err := consumer.Prepare(ctx); err != nil {
		logger.Error(ctx, "prepare notification consumer", "error", err)
		os.Exit(1)
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
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		servicemetrics.RunQueueSampler(ctx, servicemetrics.SnapshotInterval, servicemetrics.SnapshotTimeout, servicemetrics.NewRedisQueueReader(client), serviceMetrics, logger.With("component", "queue_metrics"))
	}()
	logger.Info(ctx, "notification service started", "metrics_address", cfg.MetricsAddr)
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error(ctx, "notification consumer stopped", "error", err)
		stop()
	}
	stop()
	logger.Info(context.Background(), "notification service stopping", "metrics_address", cfg.MetricsAddr)
	workers.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error(context.Background(), "metrics server shutdown failed", "address", cfg.MetricsAddr, "error", err)
	}
	cancel()
	logger.Info(context.Background(), "notification service stopped")
}

func loggerEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
