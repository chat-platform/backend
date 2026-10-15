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
}

func New(nodeID string, registry port.ConnectionRegistry, messages port.MessageService) *Gateway {
	return &Gateway{nodeID: nodeID, registry: registry, messages: messages}
}

func (g *Gateway) Register(ctx context.Context, identity domain.Identity, connID string) (domain.Connection, error) {
	if strings.TrimSpace(identity.UserID) == "" || strings.TrimSpace(identity.DeviceID) == "" || strings.TrimSpace(connID) == "" {
		return domain.Connection{}, domain.ErrInvalidIdentity
	}
	connection := domain.Connection{NodeID: g.nodeID, ConnID: connID}
	if err := g.registry.Register(ctx, identity.DeviceID, connection); err != nil {
		return domain.Connection{}, fmt.Errorf("register device connection: %w", err)
	}
	return connection, nil
}

func (g *Gateway) Refresh(ctx context.Context, deviceID string, connection domain.Connection) (bool, error) {
	return g.registry.Refresh(ctx, deviceID, connection)
}

func (g *Gateway) Unregister(ctx context.Context, deviceID string, connection domain.Connection) error {
	return g.registry.Unregister(ctx, deviceID, connection)
}

func (g *Gateway) Send(ctx context.Context, identity domain.Identity, command domain.SendMessage) (domain.MessageAck, error) {
	if strings.TrimSpace(command.ChatID) == "" || strings.TrimSpace(command.ClientMsgID) == "" || strings.TrimSpace(command.Content) == "" {
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	command.SenderID = identity.UserID
	command.DeviceID = identity.DeviceID
	return g.messages.SendMessage(ctx, command)
}
