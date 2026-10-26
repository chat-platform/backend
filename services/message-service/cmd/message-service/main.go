package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bufbuild/protovalidate-go"
	grpcserver "github.com/chat-platform/backend/services/message-service/internal/adapter/inbound/grpc"
	"github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/postgres"
	rediscache "github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/redis"
	chatrepo "github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/repository"
	"github.com/chat-platform/backend/services/message-service/internal/app"
	"github.com/chat-platform/backend/services/message-service/internal/config"
	servicemetrics "github.com/chat-platform/backend/services/message-service/internal/metrics"
	"github.com/chat-platform/backend/shared/pkg/logging"
	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := logging.New(os.Stdout, logging.Config{
		Level: os.Getenv("LOG_LEVEL"), Format: os.Getenv("LOG_FORMAT"),
		AddSource: os.Getenv("LOG_ADD_SOURCE") == "true",
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger = logger.With("service", "message-service")
	if err := run(ctx, logger); err != nil {
		logger.Error(ctx, "message-service stopped with an error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger logging.Logger) error {
	serviceCtx, stopService := context.WithCancel(ctx)
	defer stopService()
	cfg := config.FromEnv(logger)

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()

	registry, err := sharedmetrics.NewRegistry()
	if err != nil {
		return fmt.Errorf("create metrics registry: %w", err)
	}
	messageMetrics, err := servicemetrics.New(registry)
	if err != nil {
		return fmt.Errorf("register message service metrics: %w", err)
	}
	metricsServer, err := sharedmetrics.Start(cfg.MetricsAddr, registry)
	if err != nil {
		return fmt.Errorf("listen for metrics on %s: %w", cfg.MetricsAddr, err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error(context.Background(), "metrics server shutdown failed", "error", err)
		}
	}()

	metricsFailure := make(chan error, 1)
	go func() {
		if err := <-metricsServer.Done(); err != nil {
			metricsFailure <- err
			stopService()
		}
	}()

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen for gRPC on %s: %w", cfg.GRPCAddr, err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	validator, err := protovalidate.New()
	if err != nil {
		return fmt.Errorf("initialize request validator: %w", err)
	}
	postgresRepository := postgres.NewChatRepository(pool, logger.With("component", "postgres_repository"))
	chatCache := rediscache.NewChatCache(redisClient, logger.With("component", "chat_cache"))
	chatCache.StartRepairWorker(serviceCtx)
	chatRepository := chatrepo.NewCachedChatRepository(postgresRepository, chatCache, logger.With("component", "cached_chat_repository"))
	grpcserver.Register(server, app.NewChatOperations(chatRepository, logger.With("component", "chat_operations")), validator,
		logger.With("component", "grpc_server"), messageMetrics)
	logger.Info(serviceCtx, "message-service listening", "address", cfg.GRPCAddr, "metrics_address", cfg.MetricsAddr)

	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(listener) }()
	go func() {
		<-serviceCtx.Done()
		server.GracefulStop()
	}()
	var serveErr error
	select {
	case serveErr = <-serveResult:
	case <-serviceCtx.Done():
		serveErr = <-serveResult
	}
	select {
	case metricsErr := <-metricsFailure:
		return fmt.Errorf("metrics server failed: %w", metricsErr)
	default:
	}
	if serveErr != nil {
		return fmt.Errorf("serve gRPC: %w", serveErr)
	}
	if serviceCtx.Err() != nil {
		logger.Info(context.Background(), "message-service shutting down")
	}
	return nil
}
