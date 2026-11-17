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
	UserID   string
	CallID   string
	CallerID string
}

type Push struct {
	DeviceID string
	Platform string
	Token    string
	ChatID   string
	CallID   string
	CallerID string
}

type UserPushTarget struct{ DeviceID, Platform, Token string }
type UserTokenRepository interface {
	PushTokensForUser(context.Context, string) ([]UserPushTarget, error)
}

var ErrTokenUnregistered = errors.New("push token is no longer registered")

type TokenRepository interface {
	UserTokenRepository
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
	if hint.CallID != "" {
		return n.SendCall(ctx, hint)
	}
	token, platform, err := n.tokens.PushToken(ctx, hint.DeviceID)
	if err != nil {
		n.metrics.RecordTokenLookupError()
		return fmt.Errorf("lookup push token for device %s: %w", hint.DeviceID, err)
	}
	if token == "" {
		n.metrics.RecordNoTokenSkip()
		n.logger.Debug(ctx, "push hint skipped because device has no push token", "device_id", hint.DeviceID, "chat_id", hint.ChatID)
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

func (n *Notifier) SendCall(ctx context.Context, hint PushHint) error {
	targets, err := n.tokens.PushTokensForUser(ctx, hint.UserID)
	if err != nil {
		n.metrics.RecordTokenLookupError()
		return fmt.Errorf("lookup call push tokens: %w", err)
	}
	for _, target := range targets {
		if target.Token == "" {
			continue
		}
		push := Push{DeviceID: target.DeviceID, Platform: target.Platform, Token: target.Token, ChatID: hint.ChatID, CallID: hint.CallID, CallerID: hint.CallerID}
		if err := n.sender.Send(ctx, push); err != nil {
			if errors.Is(err, ErrTokenUnregistered) {
				if clearErr := n.tokens.ClearPushTokenIfCurrent(ctx, target.DeviceID, target.Token); clearErr != nil {
					return clearErr
				}
				continue
			}
			return fmt.Errorf("send call push hint: %w", err)
		}
	}
	return nil
}
