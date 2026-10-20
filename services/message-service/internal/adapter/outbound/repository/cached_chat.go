package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

const (
	allowAuthorizationTTL = 30 * time.Second
	recipientListTTL      = 30 * time.Second
)

// ChatCache is an infrastructure dependency of the cached repository.
type ChatCache interface {
	Get(context.Context, string) (string, bool, error)
	Set(context.Context, string, string, time.Duration) error
	InvalidateChatAuthorization(context.Context, string, string) error
	InvalidateChatRecipients(context.Context, string) error
}

// CachedChatRepository keeps cache-aside behavior out of application use cases.
// The wrapped repository remains the durable source of truth.
type CachedChatRepository struct {
	store  port.ChatRepository
	cache  ChatCache
	logger logging.Logger
}

func NewCachedChatRepository(store port.ChatRepository, cache ChatCache, logger logging.Logger) *CachedChatRepository {
	return &CachedChatRepository{store: store, cache: cache, logger: logger}
}

func (r *CachedChatRepository) AuthorizedChatType(ctx context.Context, chatID, userID string) (string, error) {
	key := "chat_auth:" + chatID + ":" + userID
	value, found, cacheErr := r.cache.Get(ctx, key)
	if cacheErr != nil {
		r.logger.Warn(ctx, "could not read chat authorization cache; using repository", "error", cacheErr,
			"chat_id", chatID, "user_id", userID)
	} else if found {
		switch value {
		case "DIRECT":
			return value, nil
		case "GROUP", "deny":
			if err := r.cache.InvalidateChatAuthorization(ctx, chatID, userID); err != nil {
				r.logger.Warn(ctx, "could not clear mutable group authorization cache", "error", err,
					"chat_id", chatID, "user_id", userID)
			}
		default:
			r.logger.Warn(ctx, "chat authorization cache contains an invalid decision", "chat_id", chatID,
				"user_id", userID, "decision", value)
		}
	}

	chatType, err := r.store.AuthorizedChatType(ctx, chatID, userID)
	if err != nil {
		return "", err
	}
	if chatType == "DIRECT" {
		if err := r.cache.Set(ctx, key, chatType, allowAuthorizationTTL); err != nil {
			r.logger.Warn(ctx, "could not write chat authorization cache", "error", err,
				"chat_id", chatID, "user_id", userID, "decision", chatType)
		}
	}
	return chatType, nil
}

func (r *CachedChatRepository) ResolveMessageRecipients(ctx context.Context, chatID string) ([]domain.RecipientDevice, error) {
	key := "chat_recipients:" + chatID
	value, found, cacheErr := r.cache.Get(ctx, key)
	if cacheErr != nil {
		r.logger.Warn(ctx, "could not read chat recipient cache; using repository", "error", cacheErr,
			"chat_id", chatID)
	} else if found {
		var recipients []domain.RecipientDevice
		if err := json.Unmarshal([]byte(value), &recipients); err == nil {
			if len(recipients) == 0 {
				r.logger.Warn(ctx, "chat recipient cache contains an empty list", "chat_id", chatID)
			}
			return recipients, nil
		} else {
			r.logger.Warn(ctx, "chat recipient cache contains invalid JSON", "error", err, "chat_id", chatID)
		}
	}

	recipients, err := r.store.ResolveMessageRecipients(ctx, chatID)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(recipients)
	if err != nil {
		r.logger.Error(ctx, "could not encode chat recipients for cache", "error", err, "chat_id", chatID)
	} else if err := r.cache.Set(ctx, key, string(encoded), recipientListTTL); err != nil {
		r.logger.Warn(ctx, "could not write chat recipient cache", "error", err, "chat_id", chatID)
	}
	return recipients, nil
}

func (r *CachedChatRepository) EnqueueMessageEvent(ctx context.Context, chatType string, message domain.SendMessage, recipients []domain.RecipientDevice) (domain.MessageAck, error) {
	return r.store.EnqueueMessageEvent(ctx, chatType, message, recipients)
}

func (r *CachedChatRepository) MarkDelivered(ctx context.Context, deviceID string, outboxType string, outboxID int64) error {
	return r.store.MarkDelivered(ctx, deviceID, outboxType, outboxID)
}

// InvalidateChatAuthorization removes the cached authorization decision for a user in a chat.
func (r *CachedChatRepository) InvalidateChatAuthorization(ctx context.Context, chatID, userID string) error {
	return r.cache.InvalidateChatAuthorization(ctx, chatID, userID)
}

// InvalidateChatRecipients must be called after a recipient/device mutation commits.
func (r *CachedChatRepository) InvalidateChatRecipients(ctx context.Context, chatID string) error {
	return r.cache.InvalidateChatRecipients(ctx, chatID)
}

var _ port.ChatRepository = (*CachedChatRepository)(nil)
