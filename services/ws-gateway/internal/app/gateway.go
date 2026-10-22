package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
)

type Gateway struct {
	nodeID   string
	registry port.ConnectionRegistry
	messages port.MessageService
	sessions port.SessionAuthenticator
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
	return connection, nil
}

func (g *Gateway) Refresh(ctx context.Context, deviceID string, connection domain.Connection) (bool, error) {
	registered, err := g.registry.Refresh(ctx, deviceID, connection)
	if err != nil {
		return false, fmt.Errorf("refresh websocket connection registration: %w", err)
	}
	return registered, err
}

func (g *Gateway) Unregister(ctx context.Context, deviceID string, connection domain.Connection) error {
	if err := g.registry.Unregister(ctx, deviceID, connection); err != nil {
		return fmt.Errorf("unregister websocket connection: %w", err)
	}
	return nil
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
