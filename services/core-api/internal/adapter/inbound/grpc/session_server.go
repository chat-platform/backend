package grpcserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/gen/session/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type SessionOperations interface {
	Authenticate(context.Context, string) (sessiontoken.Claims, error)
}

type PresenceOperations interface {
	GetLastSeen(context.Context, string) (*time.Time, error)
	UpdateLastSeen(context.Context, string, time.Time) error
}

type SessionServer struct {
	sessionv1.UnimplementedSessionServiceServer
	operations SessionOperations
	logger     logging.Logger
}

func RegisterSession(server *grpc.Server, operations SessionOperations, logger logging.Logger) {
	sessionv1.RegisterSessionServiceServer(server, &SessionServer{operations: operations, logger: logger})
}

func (s *SessionServer) ValidateSession(ctx context.Context, _ *sessionv1.ValidateSessionRequest) (*sessionv1.ValidateSessionResponse, error) {
	claims, err := s.authenticate(ctx, "ValidateSession")
	if err != nil {
		return nil, err
	}
	return &sessionv1.ValidateSessionResponse{
		SessionId: claims.SessionID, UserId: claims.UserID, DeviceId: claims.DeviceID,
		IssuedAt: claims.IssuedAt, ExpiresAt: claims.ExpiresAt,
	}, nil
}

func (s *SessionServer) GetLastSeen(ctx context.Context, request *sessionv1.GetLastSeenRequest) (*sessionv1.GetLastSeenResponse, error) {
	if _, err := s.authenticate(ctx, "GetLastSeen"); err != nil {
		return nil, err
	}
	presenceOps := s.presenceOperations()
	lastSeen, err := presenceOps.GetLastSeen(ctx, request.GetTargetUserId())
	if err != nil {
		s.logOperationError(ctx, "GetLastSeen", err)
		return nil, mapPresenceError(err)
	}
	response := &sessionv1.GetLastSeenResponse{}
	if lastSeen != nil {
		response.HasLastSeen = true
		response.LastSeenUnixMilli = lastSeen.UnixMilli()
	}
	return response, nil
}

// UpdateLastSeen is called only by the internal WebSocket Gateway gRPC client.
func (s *SessionServer) UpdateLastSeen(ctx context.Context, request *sessionv1.UpdateLastSeenRequest) (*sessionv1.UpdateLastSeenResponse, error) {
	if request.GetLastSeenUnixMilli() <= 0 {
		err := errors.New("lastSeenUnixMilli must be positive")
		s.logExpectedError(ctx, "UpdateLastSeen", err, codes.InvalidArgument)
		return nil, status.Error(codes.InvalidArgument, "positive lastSeenUnixMilli required")
	}
	if err := s.presenceOperations().UpdateLastSeen(ctx, request.GetUserId(), time.UnixMilli(request.GetLastSeenUnixMilli()).UTC()); err != nil {
		s.logOperationError(ctx, "UpdateLastSeen", err)
		return nil, mapPresenceError(err)
	}
	return &sessionv1.UpdateLastSeenResponse{}, nil
}

func (s *SessionServer) presenceOperations() PresenceOperations {
	operations, _ := s.operations.(PresenceOperations)
	return operations
}

func (s *SessionServer) authenticate(ctx context.Context, operation string) (sessiontoken.Claims, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		s.logExpectedError(ctx, operation, errors.New("authorization metadata missing or duplicated"), codes.Unauthenticated)
		return sessiontoken.Claims{}, status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		s.logExpectedError(ctx, operation, errors.New("authorization metadata is malformed"), codes.Unauthenticated)
		return sessiontoken.Claims{}, status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	claims, err := s.operations.Authenticate(ctx, parts[1])
	if err != nil {
		if errors.Is(err, app.ErrUnauthorized) {
			s.logExpectedError(ctx, operation, err, codes.Unauthenticated)
			return sessiontoken.Claims{}, status.Error(codes.Unauthenticated, "authentication failed")
		}
		s.logOperationError(ctx, operation, err)
		return sessiontoken.Claims{}, status.Error(codes.Internal, "session validation failed")
	}
	return claims, nil
}

func (s *SessionServer) logOperationError(ctx context.Context, operation string, err error) {
	if errors.Is(err, app.ErrInvalidRequest) || errors.Is(err, app.ErrUnauthorized) ||
		errors.Is(err, app.ErrForbidden) || errors.Is(err, app.ErrNotFound) {
		s.logExpectedError(ctx, operation, err, status.Code(mapPresenceError(err)))
		return
	}
	if s.logger != nil {
		s.logger.Error(ctx, "Core API gRPC operation failed", "operation", operation, "grpc_code", codes.Internal.String(), "error", err)
	}
}

func (s *SessionServer) logExpectedError(ctx context.Context, operation string, err error, code codes.Code) {
	if s.logger != nil {
		s.logger.Debug(ctx, "Core API gRPC request rejected", "operation", operation, "grpc_code", code.String(), "error", err)
	}
}

func mapPresenceError(err error) error {
	switch {
	case errors.Is(err, app.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, app.ErrForbidden), errors.Is(err, app.ErrUnauthorized):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, app.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, "presence operation failed")
	}
}
