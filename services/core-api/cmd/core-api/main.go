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
	"syscall"
	"time"

	grpcadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/inbound/grpc"
	httpadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/inbound/http"
	postgresadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/postgres"
	redisadapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/redis"
	s3adapter "github.com/chat-platform/backend/services/core-api/internal/adapter/outbound/s3"
	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/services/core-api/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.FromEnv()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Fatal(err)
	}
	mediaSigner, err := s3adapter.New(
		cfg.Media.Endpoint, cfg.Media.Region, cfg.Media.Bucket, cfg.Media.AccessKey, cfg.Media.SecretKey,
	)
	if err != nil {
		log.Fatalf("configure media storage: %v", err)
	}
	service := app.NewWithMedia(postgresadapter.New(pool), redisadapter.NewSessionCache(redisClient), mediaSigner, app.Config{
		OTPCode: cfg.DevOTPCode, Secret: []byte(cfg.JWTSecret), SessionTTL: cfg.SessionTTL, MediaURLTTL: cfg.Media.URLTTL,
	})
	httpServer := &http.Server{Addr: cfg.ListenAddr, Handler: httpadapter.New(service), ReadHeaderTimeout: 5 * time.Second}
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
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
