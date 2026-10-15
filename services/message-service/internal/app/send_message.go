package app

import (
	"context"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
)

type ChatOperations struct {
	chatRepository port.ChatRepository
}

func NewChatOperations(chatRepository port.ChatRepository) *ChatOperations {
	return &ChatOperations{chatRepository: chatRepository}
}

func (s *ChatOperations) SendMessage(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	ack, found, err := s.chatRepository.FindMessageEvent(ctx, command)
	if err != nil {
		return domain.MessageAck{}, err
	}
	if found {
		return ack, nil
	}

	allowed, err := s.chatRepository.IsAuthorizedToSend(ctx, command.ChatID, command.SenderID)
	if err != nil {
		return domain.MessageAck{}, err
	}
	if !allowed {
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}

	allRecipients, err := s.chatRepository.ResolveMessageRecipients(ctx, command.ChatID)
	if err != nil {
		return domain.MessageAck{}, err
	}
	recipients := make([]domain.RecipientDevice, 0, len(allRecipients))
	//removing sender device from allRecipients
	for i, recipient := range allRecipients {
		if recipient.DeviceID == command.DeviceID {
			allRecipients[i] = allRecipients[len(allRecipients)-1]
			allRecipients = allRecipients[:len(allRecipients)-1]
			break
		}
	}
	if len(recipients) == 0 {
		return domain.MessageAck{}, domain.ErrNoRecipients
	}
	return s.chatRepository.EnqueueMessageEvent(ctx, command, recipients)
}
