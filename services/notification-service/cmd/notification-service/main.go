package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	redisadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/inbound/redis"
	fcmadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/outbound/fcm"
	postgresadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/outbound/postgres"
	"github.com/chat-platform/backend/services/notification-service/internal/app"
	"github.com/chat-platform/backend/services/notification-service/internal/config"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, err := logging.New(os.Stdout, logging.Config{Level: loggerEnv("LOG_LEVEL", "info"), Format: loggerEnv("LOG_FORMAT", "json"), AddSource: os.Getenv("LOG_ADD_SOURCE") == "true"})
	if err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	host, _ := os.Hostname()
	consumerID := fmt.Sprintf("%s-%d", host, os.Getpid())
	notifier := app.New(postgresadapter.NewTokenRepository(pool), fcmadapter.NewSender(fcmClient), logger.With("component", "notifier"))
	consumer := redisadapter.NewConsumer(client, consumerID, notifier, logger.With("component", "stream_consumer"), cfg.NotificationCoalesceWindow)
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func loggerEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
