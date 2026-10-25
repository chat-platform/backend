package main

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	logger, err := logging.New(os.Stdout, logging.Config{
		Level: os.Getenv("LOG_LEVEL"), Format: os.Getenv("LOG_FORMAT"),
		AddSource: os.Getenv("LOG_ADD_SOURCE") == "true",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger = logger.With("service", "core-api")

	cfg, err := config.FromEnv()
	if err != nil {
		fatal(context.Background(), logger, "load configuration", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		fatal(ctx, logger, "connect to postgres", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fatal(ctx, logger, "ping postgres", err)
	}
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		fatal(ctx, logger, "ping redis", err)
	}
	mediaSigner, err := s3adapter.New(
		cfg.Media.Endpoint, cfg.Media.Region, cfg.Media.Bucket, cfg.Media.AccessKey, cfg.Media.SecretKey,
	)
	if err != nil {
		fatal(ctx, logger, "configure media storage", err)
	}
	service := app.NewWithMedia(postgresadapter.New(pool), redisadapter.NewSessionCache(redisClient), mediaSigner, app.Config{
		OTPCode: cfg.DevOTPCode, Secret: []byte(cfg.JWTSecret), SessionTTL: cfg.SessionTTL, MediaURLTTL: cfg.Media.URLTTL,
	})
	httpServer := &http.Server{Addr: cfg.ListenAddr, Handler: httpadapter.New(service, logger.With("component", "http_server")), ReadHeaderTimeout: 5 * time.Second}
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		fatal(ctx, logger, "listen for gRPC", err, "address", cfg.GRPCAddr)
	}
	grpcServer := grpc.NewServer()
	grpcadapter.RegisterSession(grpcServer, service, logger.With("component", "grpc_server"))
	go func() {
		<-ctx.Done()
		logger.Info(context.Background(), "core-api shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error(shutdownCtx, "core-api HTTP server shutdown failed", "error", err)
		}
		stopped := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-shutdownCtx.Done():
			logger.Error(shutdownCtx, "core-api gRPC graceful shutdown timed out", "error", shutdownCtx.Err())
			grpcServer.Stop()
		}
	}()
	go func() {
		logger.Info(ctx, "core-api gRPC listening", "address", grpcListener.Addr().String())
		if err := grpcServer.Serve(grpcListener); err != nil && ctx.Err() == nil {
			logger.Error(ctx, "core-api gRPC server failed", "error", err)
			stop()
		}
	}()
	logger.Info(ctx, "core-api HTTP listening", "address", httpServer.Addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal(ctx, logger, "core-api HTTP server failed", err)
	}
}

func fatal(ctx context.Context, logger logging.Logger, message string, err error, attrs ...any) {
	attrs = append(attrs, "error", err)
	logger.Error(ctx, message, attrs...)
	os.Exit(1)
}
