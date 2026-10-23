package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	deliveryadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/inbound/redis"
	wsadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/inbound/websocket"
	authadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/outbound/auth"
	coreapiadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/outbound/coreapi"
	grpcadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/outbound/grpc"
	redisadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/outbound/redis"
	"github.com/chat-platform/backend/services/ws-gateway/internal/app"
	"github.com/chat-platform/backend/services/ws-gateway/internal/config"
	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
	sessionv1 "github.com/chat-platform/backend/shared/gen/session/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger, err := logging.New(os.Stdout, logging.Config{
		Level: os.Getenv("LOG_LEVEL"), Format: os.Getenv("LOG_FORMAT"),
		AddSource: os.Getenv("LOG_ADD_SOURCE") == "true",
	})
	if err != nil {
		fatal(err)
	}
	logger = logger.With("service", "ws-gateway")
	cfg := config.FromEnv(logger)

	redisClient := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Error(ctx, "could not connect to redis", "error", err, "address", cfg.RedisAddr)
		os.Exit(1)
	}

	grpcConn, err := grpc.NewClient(cfg.MessageServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error(ctx, "could not create message-service gRPC client", "error", err)
		os.Exit(1)
	}
	defer grpcConn.Close()
	coreAPIConn, err := grpc.NewClient(cfg.CoreAPIAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error(ctx, "could not create Core API gRPC client", "error", err)
		os.Exit(1)
	}
	defer coreAPIConn.Close()

	registry := redisadapter.NewConnectionRegistry(redisClient)
	presenceRegistry := redisadapter.NewPresenceRegistry(redisClient)
	if len(cfg.JWTSecret) < 32 {
		logger.Error(ctx, "JWT_SECRET must contain at least 32 bytes")
		os.Exit(1)
	}
	sessionAuthenticator := authadapter.New([]byte(cfg.JWTSecret), registry, coreapiadapter.NewSessionValidator(sessionv1.NewSessionServiceClient(coreAPIConn)))
	messageService := grpcadapter.NewMessageServiceClient(messagev1.NewMessageServiceClient(grpcConn))
	presenceCoreAPI := coreapiadapter.NewPresenceClient(sessionv1.NewSessionServiceClient(coreAPIConn))
	operations := app.NewWithPresence(cfg.NodeID, registry, messageService, sessionAuthenticator, presenceRegistry, presenceCoreAPI)
	hub := wsadapter.NewHub()
	consumer := deliveryadapter.NewDeliveryConsumer(redisClient, cfg.NodeID, hub, logger.With("component", "delivery_consumer"))
	consumerFailed := make(chan error, 1)
	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error(ctx, "websocket delivery consumer stopped unexpectedly", "error", err, "node_id", cfg.NodeID)
			consumerFailed <- err
			stop()
		}
	}()
	presenceConsumer := deliveryadapter.NewPresenceConsumer(redisClient, hub, operations, logger.With("component", "presence_consumer"))
	go func() {
		if err := presenceConsumer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error(ctx, "presence Redis consumer stopped unexpectedly", "error", err, "node_id", cfg.NodeID)
		}
	}()
	mux := http.NewServeMux()
	websocketServer := wsadapter.NewServer(operations, logger.With("component", "websocket"), hub)
	mux.Handle(cfg.Path, websocketServer)
	revocations := deliveryadapter.NewRevocationConsumer(redisClient, cfg.NodeID, func(err error) {
		logger.Warn(ctx, "session revocation subscription will retry", "error", err)
	})
	go func() {
		if err := revocations.Run(ctx, func(eventCtx context.Context, connection domain.Connection) {
			websocketServer.RevokeConnection(eventCtx, connection)
		}); err != nil && ctx.Err() == nil {
			logger.Error(ctx, "session revocation listener stopped; heartbeat checks remain active", "error", err)
		}
	}()
	server := &http.Server{
		Addr: cfg.ListenAddr, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		logger.Info(context.Background(), "ws-gateway shutting down", "node_id", cfg.NodeID)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error(shutdownCtx, "http server shutdown failed", "error", err)
		}
	}()
	logger.Info(ctx, "ws-gateway listening", "address", cfg.ListenAddr, "path", cfg.Path, "node_id", cfg.NodeID)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error(ctx, "http server failed", "error", err)
		os.Exit(1)
	}
	select {
	case err := <-consumerFailed:
		logger.Error(context.Background(), "ws-gateway stopped because delivery consumer failed", "error", err, "node_id", cfg.NodeID)
		os.Exit(1)
	default:
	}
	logger.Info(context.Background(), "ws-gateway stopped", "node_id", cfg.NodeID)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
