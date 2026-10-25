package grpcserver

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	sessionv1 "github.com/chat-platform/backend/shared/gen/session/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type sessionOperationsFunc func(context.Context, string) (sessiontoken.Claims, error)

func (f sessionOperationsFunc) Authenticate(ctx context.Context, token string) (sessiontoken.Claims, error) {
	return f(ctx, token)
}

type presenceOperationsStub struct{ err error }

func (presenceOperationsStub) Authenticate(context.Context, string) (sessiontoken.Claims, error) {
	return sessiontoken.Claims{}, nil
}
func (p presenceOperationsStub) GetLastSeen(context.Context, string) (*time.Time, error) {
	return nil, p.err
}
func (p presenceOperationsStub) UpdateLastSeen(context.Context, string, time.Time) error {
	return p.err
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
	var output bytes.Buffer
	logger, err := logging.New(&output, logging.Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	server := &SessionServer{operations: sessionOperationsFunc(func(context.Context, string) (sessiontoken.Claims, error) {
		return sessiontoken.Claims{}, app.ErrUnauthorized
	}), logger: logger}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer bad-token"))
	_, err = server.ValidateSession(ctx, nil)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("error=%v", err)
	}
	if !strings.Contains(output.String(), `"level":"DEBUG"`) || strings.Contains(output.String(), "bad-token") {
		t.Fatalf("expected redacted debug authentication log, got %q", output.String())
	}
}

func TestValidateSessionMapsUnexpectedErrorToInternal(t *testing.T) {
	var output bytes.Buffer
	logger, err := logging.New(&output, logging.Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	server := &SessionServer{operations: sessionOperationsFunc(func(context.Context, string) (sessiontoken.Claims, error) {
		return sessiontoken.Claims{}, errors.New("cache unavailable")
	}), logger: logger}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer token"))
	_, err = server.ValidateSession(ctx, nil)
	if status.Code(err) != codes.Internal {
		t.Fatalf("error=%v", err)
	}
	if !strings.Contains(output.String(), `"level":"ERROR"`) || !strings.Contains(output.String(), `"operation":"ValidateSession"`) || !strings.Contains(output.String(), "cache unavailable") {
		t.Fatalf("expected contextual internal failure log, got %q", output.String())
	}
	if strings.Count(output.String(), `"msg":`) != 1 {
		t.Fatalf("expected one application log, got %q", output.String())
	}
}

func TestGetLastSeenLogsPresenceFailureWithOperationContext(t *testing.T) {
	var output bytes.Buffer
	logger, err := logging.New(&output, logging.Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	server := &SessionServer{operations: presenceOperationsStub{err: errors.New("presence database unavailable")}, logger: logger}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer valid-token"))
	_, err = server.GetLastSeen(ctx, &sessionv1.GetLastSeenRequest{TargetUserId: "target"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("error=%v", err)
	}
	logLine := output.String()
	for _, want := range []string{`"level":"ERROR"`, `"operation":"GetLastSeen"`, `"grpc_code":"Internal"`, "presence database unavailable"} {
		if !strings.Contains(logLine, want) {
			t.Errorf("log %q does not contain %s", logLine, want)
		}
	}
	if strings.Count(logLine, `"msg":`) != 1 || strings.Contains(logLine, "valid-token") {
		t.Fatalf("expected one redacted log record, got %q", logLine)
	}
}
