package coreapi

import (
	"context"
	"errors"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	sessionv1 "github.com/chat-platform/backend/shared/gen/session/v1"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type sessionClientFunc func(context.Context, *sessionv1.ValidateSessionRequest, ...grpc.CallOption) (*sessionv1.ValidateSessionResponse, error)

func (f sessionClientFunc) ValidateSession(ctx context.Context, request *sessionv1.ValidateSessionRequest, opts ...grpc.CallOption) (*sessionv1.ValidateSessionResponse, error) {
	return f(ctx, request, opts...)
}

func TestValidateSessionSendsBearerAndDecodesClaims(t *testing.T) {
	want := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: 10, ExpiresAt: 20}
	validator := NewSessionValidator(sessionClientFunc(func(ctx context.Context, _ *sessionv1.ValidateSessionRequest, _ ...grpc.CallOption) (*sessionv1.ValidateSessionResponse, error) {
		values, ok := metadata.FromOutgoingContext(ctx)
		if !ok || len(values.Get("authorization")) != 1 || values.Get("authorization")[0] != "Bearer signed-token" {
			t.Errorf("authorization metadata=%v", values)
		}
		return &sessionv1.ValidateSessionResponse{SessionId: want.SessionID, UserId: want.UserID, DeviceId: want.DeviceID, IssuedAt: want.IssuedAt, ExpiresAt: want.ExpiresAt}, nil
	}))
	got, err := validator.ValidateSession(context.Background(), "signed-token")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("claims=%+v, want %+v", got, want)
	}
}

func TestValidateSessionMapsRejectedCredentials(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		validator := NewSessionValidator(sessionClientFunc(func(context.Context, *sessionv1.ValidateSessionRequest, ...grpc.CallOption) (*sessionv1.ValidateSessionResponse, error) {
			return nil, status.Error(code, "rejected")
		}))
		if _, err := validator.ValidateSession(context.Background(), "token"); !errors.Is(err, domain.ErrInvalidIdentity) || status.Code(err) != code {
			t.Fatalf("code %s: error=%v, status=%s", code, err, status.Code(err))
		}
	}
}

func TestValidateSessionPropagatesOtherErrors(t *testing.T) {
	want := status.Error(codes.Internal, "backend unavailable")
	validator := NewSessionValidator(sessionClientFunc(func(context.Context, *sessionv1.ValidateSessionRequest, ...grpc.CallOption) (*sessionv1.ValidateSessionResponse, error) {
		return nil, want
	}))
	if _, err := validator.ValidateSession(context.Background(), "token"); !errors.Is(err, want) && status.Code(err) != codes.Internal {
		t.Fatalf("error=%v", err)
	}
}

func TestValidateSessionHonorsRequestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	validator := NewSessionValidator(sessionClientFunc(func(ctx context.Context, _ *sessionv1.ValidateSessionRequest, _ ...grpc.CallOption) (*sessionv1.ValidateSessionResponse, error) {
		return nil, ctx.Err()
	}))
	if _, err := validator.ValidateSession(ctx, "token"); err == nil {
		t.Fatal("expected canceled context error")
	}
}
