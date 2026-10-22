package app

import (
	"context"
	"fmt"

	"github.com/chat-platform/backend/shared/pkg/logging"
)

type PushHint struct {
	DeviceID string
	ChatID   string
}

type Push struct {
	DeviceID string
	Platform string
	Token    string
	ChatID   string
}

type TokenRepository interface {
	PushToken(context.Context, string) (token, platform string, err error)
}

type Sender interface {
	Send(context.Context, Push) error
}

type Notifier struct {
	tokens TokenRepository
	sender Sender
	logger logging.Logger
}

func New(tokens TokenRepository, sender Sender, logger logging.Logger) *Notifier {
	return &Notifier{tokens: tokens, sender: sender, logger: logger}
}

func (n *Notifier) Send(ctx context.Context, hint PushHint) error {
	token, platform, err := n.tokens.PushToken(ctx, hint.DeviceID)
	if err != nil {
		return fmt.Errorf("lookup push token for device %s: %w", hint.DeviceID, err)
	}
	if token == "" {
		n.logger.Info(ctx, "push hint skipped because device has no push token", "device_id", hint.DeviceID, "chat_id", hint.ChatID)
		return nil
	}
	if err := n.sender.Send(ctx, Push{DeviceID: hint.DeviceID, Platform: platform, Token: token, ChatID: hint.ChatID}); err != nil {
		return fmt.Errorf("send push hint for device %s: %w", hint.DeviceID, err)
	}
	return nil
}
