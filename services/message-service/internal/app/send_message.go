package app

import (
	"context"
	"strings"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
)

type SendMessage struct{ outbox port.Outbox }

func NewSendMessage(outbox port.Outbox) *SendMessage { return &SendMessage{outbox: outbox} }

func (s *SendMessage) Execute(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	if strings.TrimSpace(command.SenderID) == "" || strings.TrimSpace(command.DeviceID) == "" ||
		strings.TrimSpace(command.ChatID) == "" || strings.TrimSpace(command.ClientMsgID) == "" ||
		strings.TrimSpace(command.Content) == "" {
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	return s.outbox.PersistMessage(ctx, command)
}
