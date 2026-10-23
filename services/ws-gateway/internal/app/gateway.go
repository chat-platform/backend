package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/config"
	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/google/uuid"
)

type Gateway struct {
	nodeID   string
	registry port.ConnectionRegistry
	messages port.MessageService
	sessions port.SessionAuthenticator
	presence port.PresenceStore
	coreAPI  port.PresenceCoreAPI
}

func NewWithPresence(nodeID string, registry port.ConnectionRegistry, messages port.MessageService, sessions port.SessionAuthenticator, presence port.PresenceStore, coreAPI port.PresenceCoreAPI) *Gateway {
	g := New(nodeID, registry, messages, sessions)
	g.presence = presence
	g.coreAPI = coreAPI
	return g
}

func New(nodeID string, registry port.ConnectionRegistry, messages port.MessageService, sessions ...port.SessionAuthenticator) *Gateway {
	g := &Gateway{nodeID: nodeID, registry: registry, messages: messages}
	if len(sessions) > 0 {
		g.sessions = sessions[0]
	}
	return g
}

func (g *Gateway) Authenticate(ctx context.Context, token string) (domain.Identity, error) {
	if g.sessions == nil {
		return domain.Identity{}, fmt.Errorf("%w: session authenticator is not configured", domain.ErrInvalidIdentity)
	}
	return g.sessions.Authenticate(ctx, token)
}
func (g *Gateway) IsSessionActive(ctx context.Context, token string) (bool, error) {
	if g.sessions == nil {
		return false, fmt.Errorf("%w: session authenticator is not configured", domain.ErrInvalidIdentity)
	}
	return g.sessions.IsSessionActive(ctx, token)
}

func (g *Gateway) Register(ctx context.Context, identity domain.Identity, connID string) (domain.Connection, error) {
	if strings.TrimSpace(identity.UserID) == "" || strings.TrimSpace(identity.DeviceID) == "" || strings.TrimSpace(identity.SessionID) == "" || strings.TrimSpace(connID) == "" {
		return domain.Connection{}, fmt.Errorf("%w: required identity or connection fields are empty", domain.ErrInvalidIdentity)
	}
	connection := domain.Connection{NodeID: g.nodeID, ConnID: connID, SessionID: identity.SessionID}
	if err := g.registry.Register(ctx, identity.DeviceID, connection); err != nil {
		return domain.Connection{}, fmt.Errorf("register device connection: %w", err)
	}
	if g.presence != nil {
		newlyOnline, err := g.presence.RegisterConnection(ctx, identity.UserID, connID)
		if err != nil {
			_ = g.registry.Unregister(ctx, identity.DeviceID, connection)
			return domain.Connection{}, fmt.Errorf("register presence connection: %w", err)
		}
		if newlyOnline {
			if err := g.presence.Publish(ctx, domain.Presence{UserID: identity.UserID, Status: "online"}); err != nil {
				_ = g.cleanupPresenceConnection(ctx, identity.UserID, connID, time.Now().UTC())
				_ = g.registry.Unregister(ctx, identity.DeviceID, connection)
				return domain.Connection{}, fmt.Errorf("publish online presence: %w", err)
			}
		}
	}
	return connection, nil
}

func (g *Gateway) Refresh(ctx context.Context, deviceID, userID string, connection domain.Connection) (bool, error) {
	registered, err := g.registry.Refresh(ctx, deviceID, connection)
	if err != nil {
		return false, fmt.Errorf("refresh websocket connection registration: %w", err)
	}
	if !registered || g.presence == nil {
		return registered, nil
	}
	refreshed, err := g.presence.RefreshConnection(ctx, userID, connection.ConnID)
	if err != nil {
		return false, fmt.Errorf("refresh presence connection: %w", err)
	}
	return refreshed, nil
}

func (g *Gateway) Unregister(ctx context.Context, deviceID, userID string, connection domain.Connection) error {
	var cleanupErr error
	if g.presence != nil {
		cleanupErr = g.cleanupPresenceConnection(ctx, userID, connection.ConnID, time.Now().UTC())
	}
	if err := g.registry.Unregister(ctx, deviceID, connection); err != nil {
		return errors.Join(cleanupErr, fmt.Errorf("unregister websocket connection: %w", err))
	}
	return cleanupErr
}

func (g *Gateway) cleanupPresenceConnection(ctx context.Context, userID, connID string, lastSeenAt time.Time) error {
	offline, err := g.presence.RemoveConnection(ctx, userID, connID)
	if !offline {
		return err
	}
	failures := []error{err}
	if g.coreAPI != nil {
		if err := g.coreAPI.UpdateLastSeen(ctx, userID, lastSeenAt); err != nil {
			failures = append(failures, fmt.Errorf("persist last-seen: %w", err))
		}
	}
	if err := g.presence.CacheLastSeen(ctx, userID, lastSeenAt); err != nil {
		failures = append(failures, err)
	}
	if err := g.presence.Publish(ctx, domain.Presence{UserID: userID, Status: "offline", LastSeen: lastSeenAt.Format(time.RFC3339Nano)}); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func (g *Gateway) PresenceExpired(ctx context.Context, key string) error {
	userID, connID, ok := parsePresenceConnectionKey(key)
	if !ok {
		return nil
	}
	processedAt := time.Now().UTC()
	lastSeenAt := processedAt.Add(-config.HeartbeatInterval)
	return g.cleanupPresenceConnection(ctx, userID, connID, lastSeenAt)
}

func (g *Gateway) AuthorizePresence(ctx context.Context, token, targetUserID string) error {
	if _, err := uuid.Parse(targetUserID); err != nil {
		return domain.ErrInvalidMessage
	}
	if g.coreAPI == nil {
		return errors.New("presence authorization is not configured")
	}
	if err := g.coreAPI.AuthorizePresence(ctx, token, targetUserID); err != nil {
		return fmt.Errorf("authorize presence access: %w", err)
	}
	return nil
}

func (g *Gateway) SubscribePresence(ctx context.Context, token, connID, targetUserID string) error {
	if err := g.AuthorizePresence(ctx, token, targetUserID); err != nil {
		return err
	}
	if err := g.presence.Subscribe(ctx, targetUserID, connID); err != nil {
		return err
	}
	return nil
}

func (g *Gateway) UnsubscribePresence(ctx context.Context, connID, targetUserID string) error {
	if _, err := uuid.Parse(targetUserID); err != nil {
		return domain.ErrInvalidMessage
	}
	return g.presence.Unsubscribe(ctx, targetUserID, connID)
}

func (g *Gateway) GetPresence(ctx context.Context, token, targetUserID string) (domain.Presence, error) {
	if err := g.AuthorizePresence(ctx, token, targetUserID); err != nil {
		return domain.Presence{}, err
	}
	return g.readPresence(ctx, token, targetUserID)
}

func (g *Gateway) readPresence(ctx context.Context, token, targetUserID string) (domain.Presence, error) {
	online, err := g.presence.IsOnline(ctx, targetUserID)
	if err != nil {
		return domain.Presence{}, err
	}
	if online {
		return domain.Presence{UserID: targetUserID, Status: "online"}, nil
	}
	lastSeen, err := g.presence.CachedLastSeen(ctx, targetUserID)
	if err != nil {
		return domain.Presence{}, err
	}
	if lastSeen == nil && g.coreAPI != nil {
		lastSeen, err = g.coreAPI.GetLastSeen(ctx, token, targetUserID)
		if err != nil {
			return domain.Presence{}, err
		}
		if lastSeen != nil {
			if err := g.presence.CacheLastSeen(ctx, targetUserID, *lastSeen); err != nil {
				return domain.Presence{}, err
			}
		}
	}
	result := domain.Presence{UserID: targetUserID, Status: "offline"}
	if lastSeen != nil {
		result.LastSeen = lastSeen.UTC().Format(time.RFC3339Nano)
	}
	return result, nil
}

func parsePresenceConnectionKey(key string) (string, string, bool) {
	const prefix = "presence:conn:"
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, prefix), ":")
	if len(parts) != 2 {
		return "", "", false
	}
	userID, userErr := uuid.Parse(parts[0])
	connID, connErr := uuid.Parse(parts[1])
	if userErr != nil || connErr != nil {
		return "", "", false
	}
	return userID.String(), connID.String(), true
}

func (g *Gateway) Send(ctx context.Context, identity domain.Identity, command domain.SendMessage) (domain.MessageAck, error) {
	if strings.TrimSpace(command.ChatID) == "" || strings.TrimSpace(command.ClientMsgID) == "" || strings.TrimSpace(command.Content) == "" {
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	command.SenderID = identity.UserID
	command.DeviceID = identity.DeviceID
	ack, err := g.messages.SendMessage(ctx, command)
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("send direct message: %w", err)
	}
	return ack, nil
}

func (g *Gateway) MarkDelivered(ctx context.Context, identity domain.Identity, outboxType string, outboxID int64) error {
	if outboxID <= 0 {
		return domain.ErrInvalidMessage
	}
	if err := g.messages.MarkDelivered(ctx, identity.DeviceID, outboxType, outboxID); err != nil {
		return fmt.Errorf("record delivered receipt: %w", err)
	}
	return nil
}

func (g *Gateway) MarkRead(ctx context.Context, identity domain.Identity, outboxType string, outboxID int64, readAt int64) error {
	if (outboxType != "direct" && outboxType != "group") || outboxID <= 0 || readAt <= 0 {
		return domain.ErrInvalidMessage
	}
	if err := g.messages.MarkRead(ctx, identity.DeviceID, outboxType, outboxID, readAt); err != nil {
		return fmt.Errorf("record read receipt: %w", err)
	}
	return nil
}
