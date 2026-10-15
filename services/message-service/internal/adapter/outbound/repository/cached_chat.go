package repository

import (
	"context"
	"encoding/json"
	"time"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
)

const (
	allowAuthorizationTTL = 30 * time.Second
	denyAuthorizationTTL  = 5 * time.Second
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
	store port.ChatRepository
	cache ChatCache
}

func NewCachedChatRepository(store port.ChatRepository, cache ChatCache) *CachedChatRepository {
	return &CachedChatRepository{store: store, cache: cache}
}

func (r *CachedChatRepository) IsAuthorizedToSend(ctx context.Context, chatID, userID string) (bool, error) {
	key := "chat_auth:" + chatID + ":" + userID
	if value, found, err := r.cache.Get(ctx, key); err == nil && found {
		switch value {
		case "allow":
			return true, nil
		case "deny":
			return false, nil
		}
	}

	allowed, err := r.store.IsAuthorizedToSend(ctx, chatID, userID)
	if err != nil {
		return false, err
	}
	value, ttl := "deny", denyAuthorizationTTL
	if allowed {
		value, ttl = "allow", allowAuthorizationTTL
	}
	_ = r.cache.Set(ctx, key, value, ttl)
	return allowed, nil
}

func (r *CachedChatRepository) ResolveMessageRecipients(ctx context.Context, chatID string) ([]domain.RecipientDevice, error) {
	key := "chat_recipients:" + chatID
	if value, found, err := r.cache.Get(ctx, key); err == nil && found {
		var recipients []domain.RecipientDevice
		if err := json.Unmarshal([]byte(value), &recipients); err == nil {
			return recipients, nil
		}
	}

	recipients, err := r.store.ResolveMessageRecipients(ctx, chatID)
	if err != nil {
		return nil, err
	}
	if encoded, err := json.Marshal(recipients); err == nil {
		_ = r.cache.Set(ctx, key, string(encoded), recipientListTTL)
	}
	return recipients, nil
}

func (r *CachedChatRepository) EnqueueMessageEvent(ctx context.Context, message domain.SendMessage, recipients []domain.RecipientDevice) (domain.MessageAck, error) {
	return r.store.EnqueueMessageEvent(ctx, message, recipients)
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
