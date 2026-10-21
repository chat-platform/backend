package mock

import (
	"context"

	"github.com/chat-platform/backend/services/notification-service/internal/app"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Sender struct{ logger logging.Logger }

func NewSender(logger logging.Logger) *Sender { return &Sender{logger: logger} }

func (s *Sender) Send(ctx context.Context, push app.Push) error {
	// Intentionally omit the token and message payload from logs.
	s.logger.Info(ctx, "mock push-to-sync hint sent", "device_id", push.DeviceID, "platform", push.Platform, "chat_id", push.ChatID)
	return nil
}
