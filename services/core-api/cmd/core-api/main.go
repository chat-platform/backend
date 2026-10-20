package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	httpadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/inbound/http"
	postgresadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/postgres"
	redisadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/redis"
	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) < 32 {
		log.Fatal("JWT_SECRET must contain at least 32 bytes")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, env("POSTGRES_DSN", "postgres://messenger:messenger@localhost:5432/messenger?sslmode=disable"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	redisClient := redis.NewClient(&redis.Options{Addr: env("REDIS_ADDR", "localhost:6379")})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	ttl, err := time.ParseDuration(env("SESSION_TTL", "24h"))
	if err != nil || ttl <= 0 {
		log.Fatal("SESSION_TTL must be a positive duration")
	}
	service := app.New(postgresadapter.New(pool), redisadapter.NewSessionCache(redisClient), app.Config{
		OTPCode: env("DEV_OTP_CODE", "000000"), Secret: secret, SessionTTL: ttl,
	})
	server := &http.Server{Addr: env("LISTEN_ADDR", ":8081"), Handler: httpadapter.New(service), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	fmt.Printf("core-api listening on %s\n", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
