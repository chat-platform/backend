package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	redisadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/inbound/redis"
	fcmadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/outbound/fcm"
	postgresadapter "github.com/chat-platform/backend/services/notification-service/internal/adapter/outbound/postgres"
	"github.com/chat-platform/backend/services/notification-service/internal/app"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger, err := logging.New(os.Stdout, logging.Config{Level: env("LOG_LEVEL", "info"), Format: env("LOG_FORMAT", "json"), AddSource: os.Getenv("LOG_ADD_SOURCE") == "true"})
	if err != nil {
		log.Fatal(err)
	}
	logger = logger.With("service", "notification-service")
	fcmClient, err := fcmadapter.NewADCClient(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		logger.Error(ctx, "initialize FCM client", "error", err)
		os.Exit(1)
	}
	pool, err := pgxpool.New(ctx, env("POSTGRES_DSN", "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	host, _ := os.Hostname()
	consumerID := fmt.Sprintf("%s-%d", host, os.Getpid())
	notifier := app.New(postgresadapter.NewTokenRepository(pool), fcmadapter.NewSender(fcmClient), logger.With("component", "notifier"))
	consumer := redisadapter.NewConsumer(client, consumerID, notifier, logger.With("component", "stream_consumer"), durationEnv("NOTIFICATION_COALESCE_WINDOW", 5*time.Second))
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		log.Fatalf("%s must be a positive duration", key)
	}
	return d
}
