package app

import (
	"context"
	"errors"
	"testing"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

type tokenRepo struct {
	token, platform string
	err             error
	clearedDevice   string
	clearedToken    string
	clearErr        error
}

func (r *tokenRepo) PushToken(context.Context, string) (string, string, error) {
	return r.token, r.platform, r.err
}

func (r *tokenRepo) ClearPushTokenIfCurrent(_ context.Context, deviceID, token string) error {
	r.clearedDevice, r.clearedToken = deviceID, token
	return r.clearErr
}

type sender struct {
	pushes []Push
	err    error
}

func (s *sender) Send(_ context.Context, push Push) error {
	s.pushes = append(s.pushes, push)
	return s.err
}

func TestNotifierSendsOpaqueChatHintAndSkipsMissingToken(t *testing.T) {
	logger, _ := logging.New(&discard{}, logging.Config{Level: "error"})
	out := &sender{}
	notifier := New(&tokenRepo{token: "device-token", platform: "IOS"}, out, logger)
	if err := notifier.Send(context.Background(), PushHint{DeviceID: "device", ChatID: "opaque-chat"}); err != nil {
		t.Fatal(err)
	}
	if len(out.pushes) != 1 || out.pushes[0].ChatID != "opaque-chat" || out.pushes[0].Token != "device-token" || out.pushes[0].Platform != "IOS" {
		t.Fatalf("pushes=%+v", out.pushes)
	}
	out.pushes = nil
	if err := New(&tokenRepo{}, out, logger).Send(context.Background(), PushHint{DeviceID: "device", ChatID: "opaque-chat"}); err != nil {
		t.Fatal(err)
	}
	if len(out.pushes) != 0 {
		t.Fatalf("push without token sent: %+v", out.pushes)
	}
}

func TestNotifierPropagatesProviderAndRepositoryFailures(t *testing.T) {
	logger, _ := logging.New(&discard{}, logging.Config{Level: "error"})
	if err := New(&tokenRepo{err: errors.New("database down")}, &sender{}, logger).Send(context.Background(), PushHint{}); err == nil {
		t.Fatal("expected repository error")
	}
	if err := New(&tokenRepo{token: "token"}, &sender{err: errors.New("provider down")}, logger).Send(context.Background(), PushHint{}); err == nil {
		t.Fatal("expected sender error")
	}
}

func TestNotifierClearsOnlyTheUnregisteredTokenAndAcknowledges(t *testing.T) {
	logger, _ := logging.New(&discard{}, logging.Config{Level: "error"})
	repo := &tokenRepo{token: "old-token", platform: "ANDROID"}
	out := &sender{err: ErrTokenUnregistered}
	if err := New(repo, out, logger).Send(context.Background(), PushHint{DeviceID: "device-1", ChatID: "chat-1"}); err != nil {
		t.Fatalf("unregistered token should be treated as handled: %v", err)
	}
	if repo.clearedDevice != "device-1" || repo.clearedToken != "old-token" {
		t.Fatalf("cleared device/token=%q/%q", repo.clearedDevice, repo.clearedToken)
	}
}

func TestNotifierKeepsStreamPendingWhenStaleTokenCleanupFails(t *testing.T) {
	logger, _ := logging.New(&discard{}, logging.Config{Level: "error"})
	repo := &tokenRepo{token: "old-token", clearErr: errors.New("database unavailable")}
	out := &sender{err: ErrTokenUnregistered}
	if err := New(repo, out, logger).Send(context.Background(), PushHint{DeviceID: "device-1", ChatID: "chat-1"}); err == nil {
		t.Fatal("expected cleanup error to propagate for stream retry")
	}
}

type discard struct{}

func (*discard) Write(p []byte) (int, error) { return len(p), nil }
