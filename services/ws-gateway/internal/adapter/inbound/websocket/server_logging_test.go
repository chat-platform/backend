package websocket

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lockedLogBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(data)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func TestLogOperationFailureClassifiesAndLogsOnce(t *testing.T) {
	var output bytes.Buffer
	logger, err := logging.New(&output, logging.Config{Level: "debug", Format: "json"})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{logger: logger}
	invalidIdentity := fmt.Errorf("%w: verify token: %w", domain.ErrInvalidIdentity, sessiontoken.ErrInvalidToken)
	server.logOperationFailure(context.Background(), "authenticate websocket", invalidIdentity, "remote_addr", "127.0.0.1")
	if lines := strings.Count(strings.TrimSpace(output.String()), "\n") + 1; lines != 1 {
		t.Fatalf("expected one log record, got %d: %s", lines, output.String())
	}
	if !strings.Contains(output.String(), `"level":"DEBUG"`) {
		t.Fatalf("expected invalid identity at DEBUG level: %s", output.String())
	}
	if strings.Contains(output.String(), "bearer") || strings.Contains(output.String(), "token-value") {
		t.Fatalf("log contains credential material: %s", output.String())
	}

	output.Reset()
	dependencyErr := fmt.Errorf("message service SendMessage RPC: %w", status.Error(codes.Internal, "database unavailable"))
	server.logOperationFailure(context.Background(), "send message", dependencyErr, "device_id", "device-1")
	if lines := strings.Count(strings.TrimSpace(output.String()), "\n") + 1; lines != 1 {
		t.Fatalf("expected one dependency log record, got %d: %s", lines, output.String())
	}
	if !strings.Contains(output.String(), `"level":"ERROR"`) || !strings.Contains(output.String(), `"grpc_code":"Internal"`) {
		t.Fatalf("expected ERROR with gRPC status: %s", output.String())
	}
	if errors.Is(dependencyErr, domain.ErrInvalidIdentity) {
		t.Fatal("dependency failure unexpectedly classified as invalid identity")
	}
}
