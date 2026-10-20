package grpcserver

import (
	"context"
	"errors"
	"testing"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type sessionOperationsFunc func(context.Context, string) (sessiontoken.Claims, error)

func (f sessionOperationsFunc) Authenticate(ctx context.Context, token string) (sessiontoken.Claims, error) {
	return f(ctx, token)
}

func TestValidateSessionReturnsClaims(t *testing.T) {
	want := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: 10, ExpiresAt: 20}
	server := &SessionServer{operations: sessionOperationsFunc(func(_ context.Context, token string) (sessiontoken.Claims, error) {
		if token != "signed-token" {
			t.Fatalf("token=%q", token)
		}
		return want, nil
	})}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer signed-token"))
	got, err := server.ValidateSession(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.GetSessionId() != want.SessionID || got.GetUserId() != want.UserID || got.GetDeviceId() != want.DeviceID || got.GetIssuedAt() != want.IssuedAt || got.GetExpiresAt() != want.ExpiresAt {
		t.Fatalf("response=%+v", got)
	}
}

func TestValidateSessionRejectsInvalidCredentials(t *testing.T) {
	server := &SessionServer{operations: sessionOperationsFunc(func(context.Context, string) (sessiontoken.Claims, error) {
		return sessiontoken.Claims{}, app.ErrUnauthorized
	})}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer bad-token"))
	_, err := server.ValidateSession(ctx, nil)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error=%v", err)
	}
}

func TestValidateSessionMapsUnexpectedErrorToInternal(t *testing.T) {
	server := &SessionServer{operations: sessionOperationsFunc(func(context.Context, string) (sessiontoken.Claims, error) {
		return sessiontoken.Claims{}, errors.New("cache unavailable")
	})}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token"))
	_, err := server.ValidateSession(ctx, nil)
	if status.Code(err) != codes.Internal {
		t.Fatalf("error=%v", err)
	}
}
