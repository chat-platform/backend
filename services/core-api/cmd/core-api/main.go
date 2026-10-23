package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	grpcadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/inbound/grpc"
	httpadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/inbound/http"
	postgresadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/postgres"
	redisadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/redis"
	s3adapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/s3"
	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
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
	mediaTTL, err := time.ParseDuration(env("MEDIA_URL_TTL", "5m"))
	if err != nil || mediaTTL < time.Second || mediaTTL > 7*24*time.Hour {
		log.Fatal("MEDIA_URL_TTL must be between 1 second and 7 days")
	}
	mediaSigner, err := s3adapter.New(
		env("MEDIA_S3_ENDPOINT", "https://s3."+env("MEDIA_S3_REGION", "us-east-1")+".amazonaws.com"),
		env("MEDIA_S3_REGION", "us-east-1"),
		env("MEDIA_S3_BUCKET", ""),
		env("MEDIA_S3_ACCESS_KEY", ""),
		env("MEDIA_S3_SECRET_KEY", ""),
	)
	if err != nil {
		log.Fatalf("configure media storage: %v", err)
	}
	service := app.NewWithMedia(postgresadapter.New(pool), redisadapter.NewSessionCache(redisClient), mediaSigner, app.Config{
		OTPCode: env("DEV_OTP_CODE", "000000"), Secret: secret, SessionTTL: ttl, MediaURLTTL: mediaTTL,
	})
	httpServer := &http.Server{Addr: env("LISTEN_ADDR", ":8081"), Handler: httpadapter.New(service), ReadHeaderTimeout: 5 * time.Second}
	grpcListener, err := net.Listen("tcp", env("GRPC_ADDR", ":50052"))
	if err != nil {
		log.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	grpcadapter.RegisterSession(grpcServer, service)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		stopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			grpcServer.Stop()
		}
	}()
	go func() {
		fmt.Printf("core-api gRPC listening on %s\n", grpcListener.Addr())
		if err := grpcServer.Serve(grpcListener); err != nil && ctx.Err() == nil {
			log.Printf("core-api gRPC server failed: %v", err)
			stop()
		}
	}()
	fmt.Printf("core-api HTTP listening on %s\n", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
