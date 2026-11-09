package app

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type ChatOperations struct {
	chatRepository port.ChatRepository
	logger         logging.Logger
}

func NewChatOperations(chatRepository port.ChatRepository, logger logging.Logger) *ChatOperations {
	return &ChatOperations{chatRepository: chatRepository, logger: logger}
}

func (s *ChatOperations) SendMessage(ctx context.Context, command domain.SendMessage) (domain.MessageAck, error) {
	chatType, err := s.chatRepository.AuthorizedChatType(ctx, command.ChatID, command.SenderID)
	if err != nil {
		return domain.MessageAck{}, err
	}
	if chatType == "" {
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}

	switch chatType {
	case "DIRECT":
		allRecipients, err := s.chatRepository.ResolveMessageRecipients(ctx, command.ChatID)
		if err != nil && !errors.Is(err, domain.ErrNoRecipients) {
			return domain.MessageAck{}, err
		}
		if errors.Is(err, domain.ErrNoRecipients) {
			allRecipients = nil
		}
		recipients := make([]domain.RecipientDevice, 0, len(allRecipients))
		for _, recipient := range allRecipients {
			if recipient.DeviceID != command.DeviceID {
				recipients = append(recipients, recipient)
			}
		}
		if len(recipients) == 0 {
			s.logger.Info(ctx, "direct message accepted without recipient devices; discarding",
				"chat_id", command.ChatID,
				"resolved_devices", len(allRecipients),
			)
			return domain.MessageAck{ClientMsgID: command.ClientMsgID, TimestampMS: time.Now().UnixMilli()}, nil
		}
		return s.chatRepository.EnqueueDirectMessageEvent(ctx, command, recipients)
	case "GROUP":
		return s.chatRepository.EnqueueGroupMessageEvent(ctx, command)
	default:
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}
}

func (s *ChatOperations) MarkDelivered(ctx context.Context, receipt domain.Receipt) (domain.DeliveryAck, error) {
	if receipt.DeviceID == "" || receipt.ReceiptSenderID == "" || (receipt.OutboxType != "direct" && receipt.OutboxType != "group") || receipt.OutboxID <= 0 || receipt.ChatID == "" || receipt.MessageSenderID == "" || receipt.ClientMsgID == "" {
		return domain.DeliveryAck{}, domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkDelivered(ctx, receipt)
}

func (s *ChatOperations) DeleteForEveryone(ctx context.Context, message domain.DeleteMessage) error {
	if message.SenderID == "" || message.DeviceID == "" || message.ChatID == "" || message.ClientMsgID == "" {
		return domain.ErrInvalidMessage
	}
	chatType, err := s.chatRepository.AuthorizedChatType(ctx, message.ChatID, message.SenderID)
	if err != nil {
		return err
	}
	if chatType != "DIRECT" && chatType != "GROUP" {
		return domain.ErrChatAccessDenied
	}
	return s.chatRepository.DeleteForEveryone(ctx, message, chatType)
}

func (s *ChatOperations) ReactToMessage(ctx context.Context, reaction domain.ReactToMessage) (domain.ReactionAck, error) {
	if reaction.ReactorID == "" || reaction.DeviceID == "" || reaction.ChatID == "" ||
		reaction.TargetSenderID == "" || reaction.TargetClientMsgID == "" ||
		reaction.ClientReactionID == "" || reaction.TimestampMS <= 0 {
		return domain.ReactionAck{}, domain.ErrInvalidMessage
	}
	if reaction.Reaction != nil && !isSingleEmoji(*reaction.Reaction) {
		return domain.ReactionAck{}, domain.ErrInvalidMessage
	}
	if err := s.chatRepository.EnqueueReaction(ctx, reaction); err != nil {
		return domain.ReactionAck{}, err
	}
	return domain.ReactionAck{ClientReactionID: reaction.ClientReactionID, TimestampMS: reaction.TimestampMS}, nil
}

func isSingleEmoji(value string) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value || len(value) == 0 || len(value) > 32 {
		return false
	}
	runes := []rune(value)
	if len(runes) == 2 && unicode.Is(unicode.Regional_Indicator, runes[0]) && unicode.Is(unicode.Regional_Indicator, runes[1]) {
		return true
	}
	if len(runes) >= 2 && (runes[0] == '#' || runes[0] == '*' || (runes[0] >= '0' && runes[0] <= '9')) {
		i := 1
		if i < len(runes) && runes[i] == '\uFE0F' {
			i++
		}
		return i == len(runes)-1 && runes[i] == '\u20E3'
	}

	hasBase := false
	needBase := true
	for i := 0; i < len(runes); {
		r := runes[i]
		if needBase {
			if !unicode.Is(unicode.So, r) {
				return false
			}
			hasBase = true
			needBase = false
			i++
			continue
		}
		switch {
		case r == '\uFE0F':
			i++
		case r >= 0x1F3FB && r <= 0x1F3FF:
			i++
		case r >= 0xE0020 && r <= 0xE007E:
			i++
		case r == 0xE007F:
			i++
		case r == '\u200D':
			needBase = true
			i++
		default:
			return false
		}
	}
	return hasBase && !needBase
}

func (s *ChatOperations) MarkRead(ctx context.Context, receipt domain.Receipt) error {
	if receipt.DeviceID == "" || receipt.ReceiptSenderID == "" || receipt.ChatID == "" || receipt.MessageSenderID == "" || receipt.ClientMsgID == "" {
		return domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkRead(ctx, receipt)
}

func (s *ChatOperations) MarkPlayed(ctx context.Context, receipt domain.Receipt) error {
	if receipt.DeviceID == "" || receipt.ReceiptSenderID == "" || receipt.ChatID == "" || receipt.MessageSenderID == "" || receipt.ClientMsgID == "" {
		return domain.ErrInvalidMessage
	}
	return s.chatRepository.MarkPlayed(ctx, receipt)
}
