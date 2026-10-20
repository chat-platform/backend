package grpcserver

import (
	"context"
	"errors"
	"strings"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/gen/session/v1"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type SessionOperations interface {
	Authenticate(context.Context, string) (sessiontoken.Claims, error)
}

type SessionServer struct {
	sessionv1.UnimplementedSessionServiceServer
	operations SessionOperations
}

func RegisterSession(server *grpc.Server, operations SessionOperations) {
	sessionv1.RegisterSessionServiceServer(server, &SessionServer{operations: operations})
}

func (s *SessionServer) ValidateSession(ctx context.Context, _ *sessionv1.ValidateSessionRequest) (*sessionv1.ValidateSessionResponse, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return nil, status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	token := parts[1]
	claims, err := s.operations.Authenticate(ctx, token)
	if err != nil {
		if errors.Is(err, app.ErrUnauthorized) {
			return nil, status.Error(codes.Unauthenticated, "authentication failed")
		}
		return nil, status.Error(codes.Internal, "session validation failed")
	}
	return &sessionv1.ValidateSessionResponse{
		SessionId: claims.SessionID, UserId: claims.UserID, DeviceId: claims.DeviceID,
		IssuedAt: claims.IssuedAt, ExpiresAt: claims.ExpiresAt,
	}, nil
}
