package port

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
)

type ChatRepository interface {
	AuthorizedChatType(context.Context, string, string) (string, error)
	ResolveMessageRecipients(context.Context, string) ([]domain.RecipientDevice, error)
	EnqueueDirectMessageEvent(context.Context, domain.SendMessage, []domain.RecipientDevice) (domain.MessageAck, error)
	EnqueueGroupMessageEvent(context.Context, domain.SendMessage) (domain.MessageAck, error)
	MarkDelivered(context.Context, domain.Receipt) (domain.DeliveryAck, error)
	MarkRead(context.Context, domain.Receipt) error
	MarkPlayed(context.Context, domain.Receipt) error
}
