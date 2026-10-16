package app

import (
	"context"
	"fmt"
	"strings"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Gateway struct {
	nodeID   string
	registry port.ConnectionRegistry
	messages port.MessageService
	logger   logging.Logger
}

func New(nodeID string, registry port.ConnectionRegistry, messages port.MessageService, logger logging.Logger) *Gateway {
	return &Gateway{nodeID: nodeID, registry: registry, messages: messages, logger: logger}
}

func (g *Gateway) Register(ctx context.Context, identity domain.Identity, connID string) (domain.Connection, error) {
	if strings.TrimSpace(identity.UserID) == "" || strings.TrimSpace(identity.DeviceID) == "" || strings.TrimSpace(connID) == "" {
		g.logger.Warn(ctx, "websocket connection registration rejected for invalid identity",
			"user_id", identity.UserID, "device_id", identity.DeviceID, "conn_id", connID)
		return domain.Connection{}, domain.ErrInvalidIdentity
	}
	connection := domain.Connection{NodeID: g.nodeID, ConnID: connID}
	if err := g.registry.Register(ctx, identity.DeviceID, connection); err != nil {
		g.logger.Error(ctx, "could not register websocket connection", "error", err,
			"user_id", identity.UserID, "device_id", identity.DeviceID,
			"ws_node", connection.NodeID, "conn_id", connection.ConnID)
		return domain.Connection{}, fmt.Errorf("register device connection: %w", err)
	}
	return connection, nil
}

func (g *Gateway) Refresh(ctx context.Context, deviceID string, connection domain.Connection) (bool, error) {
	registered, err := g.registry.Refresh(ctx, deviceID, connection)
	if err != nil {
		g.logger.Error(ctx, "could not refresh websocket connection registration", "error", err,
			"device_id", deviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID)
	}
	return registered, err
}

func (g *Gateway) Unregister(ctx context.Context, deviceID string, connection domain.Connection) error {
	if err := g.registry.Unregister(ctx, deviceID, connection); err != nil {
		g.logger.Error(ctx, "could not unregister websocket connection", "error", err,
			"device_id", deviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID)
		return err
	}
	return nil
}

func (g *Gateway) Send(ctx context.Context, identity domain.Identity, command domain.SendMessage) (domain.MessageAck, error) {
	if strings.TrimSpace(command.ChatID) == "" || strings.TrimSpace(command.ClientMsgID) == "" || strings.TrimSpace(command.Content) == "" {
		g.logger.Warn(ctx, "message send rejected for invalid command",
			"sender_id", identity.UserID, "device_id", identity.DeviceID,
			"chat_id", command.ChatID, "client_msg_id", command.ClientMsgID)
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	command.SenderID = identity.UserID
	command.DeviceID = identity.DeviceID
	return g.messages.SendMessage(ctx, command)
}
