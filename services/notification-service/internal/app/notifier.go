package app

import (
	"context"
	"errors"
	"fmt"

	servicemetrics "github.com/chat-platform/backend/services/notification-service/internal/metrics"
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

var ErrTokenUnregistered = errors.New("push token is no longer registered")

type TokenRepository interface {
	PushToken(context.Context, string) (token, platform string, err error)
	ClearPushTokenIfCurrent(context.Context, string, string) error
}

type Sender interface {
	Send(context.Context, Push) error
}

type Notifier struct {
	tokens  TokenRepository
	sender  Sender
	logger  logging.Logger
	metrics *servicemetrics.Metrics
}

func New(tokens TokenRepository, sender Sender, logger logging.Logger, metrics *servicemetrics.Metrics) *Notifier {
	return &Notifier{tokens: tokens, sender: sender, logger: logger, metrics: metrics}
}

func (n *Notifier) Send(ctx context.Context, hint PushHint) error {
	token, platform, err := n.tokens.PushToken(ctx, hint.DeviceID)
	if err != nil {
		n.metrics.RecordTokenLookupError()
		return fmt.Errorf("lookup push token for device %s: %w", hint.DeviceID, err)
	}
	if token == "" {
		n.metrics.RecordNoTokenSkip()
		n.logger.Info(ctx, "push hint skipped because device has no push token", "device_id", hint.DeviceID, "chat_id", hint.ChatID)
		return nil
	}
	if err := n.sender.Send(ctx, Push{DeviceID: hint.DeviceID, Platform: platform, Token: token, ChatID: hint.ChatID}); err != nil {
		if errors.Is(err, ErrTokenUnregistered) {
			if clearErr := n.tokens.ClearPushTokenIfCurrent(ctx, hint.DeviceID, token); clearErr != nil {
				return fmt.Errorf("clear unregistered push token for device %s: %w", hint.DeviceID, clearErr)
			}
			n.logger.Warn(ctx, "unregistered push token cleared", "device_id", hint.DeviceID)
			return nil
		}
		return fmt.Errorf("send push hint for device %s: %w", hint.DeviceID, err)
	}
	return nil
}
