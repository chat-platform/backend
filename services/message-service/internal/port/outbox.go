package port

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
)

type Outbox interface {
	PersistMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
}
