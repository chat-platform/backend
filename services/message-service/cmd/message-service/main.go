package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/bufbuild/protovalidate-go"
	grpcserver "github.com/chat-platform/backend/services/message-service/internal/adapter/inbound/grpc"
	"github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/postgres"
	rediscache "github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/redis"
	chatrepo "github.com/chat-platform/backend/services/message-service/internal/adapter/outbound/repository"
	"github.com/chat-platform/backend/services/message-service/internal/app"
	"github.com/chat-platform/backend/services/message-service/internal/config"
	"github.com/chat-platform/backend/shared/pkg/logging"
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
	cfg := config.FromEnv(logger)

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

	listener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		fatal(ctx, logger, "listen for gRPC", err, "address", cfg.GRPCAddr)
	}
	server := grpc.NewServer()
	validator, err := protovalidate.New()
	if err != nil {
		fatal(ctx, logger, "initialize request validator", err)
	}
	postgresRepository := postgres.NewChatRepository(pool, logger.With("component", "postgres_repository"))
	ChatCache := rediscache.NewChatCache(redisClient, logger.With("component", "chat_cache"))
	ChatCache.StartRepairWorker(ctx)
	chatRepository := chatrepo.NewCachedChatRepository(postgresRepository, ChatCache, logger.With("component", "cached_chat_repository"))
	grpcserver.Register(server, app.NewChatOperations(chatRepository, logger.With("component", "chat_operations")), validator, logger.With("component", "grpc_server"))
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	logger.Info(ctx, "message-service listening", "address", cfg.GRPCAddr)
	if err := server.Serve(listener); err != nil {
		fatal(ctx, logger, "serve gRPC", err)
	}
}

func fatal(ctx context.Context, logger logging.Logger, message string, err error, attrs ...any) {
	attrs = append(attrs, "error", err)
	logger.Error(ctx, message, attrs...)
	os.Exit(1)
}
