package app

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
)

type SendMessage struct{ outbox port.Outbox }

func NewSendMessage(outbox port.Outbox) *SendMessage { return &SendMessage{outbox: outbox} }

func (s *SendMessage) Execute(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	return s.outbox.PersistMessage(ctx, command)
}
