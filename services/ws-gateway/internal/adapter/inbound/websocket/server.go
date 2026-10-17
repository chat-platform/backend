package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	identityTimeout = 10 * time.Second
	heartbeatEvery  = 20 * time.Second
	readTimeout     = 60 * time.Second
	maxFrameBytes   = 1 << 20
)

type Operations interface {
	Register(context.Context, domain.Identity, string) (domain.Connection, error)
	Refresh(context.Context, string, domain.Connection) (bool, error)
	Unregister(context.Context, string, domain.Connection) error
	Send(context.Context, domain.Identity, domain.SendMessage) (domain.MessageAck, error)
}

type Server struct {
	operations Operations
	hub        *Hub
	logger     logging.Logger
	upgrader   websocket.Upgrader
}

func NewServer(operations Operations, logger logging.Logger, hub *Hub) *Server {
	return &Server{
		operations: operations,
		hub:        hub,
		logger:     logger,
		upgrader: websocket.Upgrader{
			HandshakeTimeout: 10 * time.Second,
		},
	}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	conn, err := s.upgrader.Upgrade(writer, request, nil)
	if err != nil {
		s.logger.Warn(request.Context(), "websocket upgrade failed", "error", err)
		return
	}
	socket := &safeSocket{conn: conn}
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Warn(context.Background(), "could not close websocket connection", "error", err,
				"remote_addr", request.RemoteAddr)
		}
	}()
	conn.SetReadLimit(maxFrameBytes)
	if err := conn.SetReadDeadline(time.Now().Add(identityTimeout)); err != nil {
		s.logger.Error(request.Context(), "could not set websocket identity read deadline", "error", err,
			"remote_addr", request.RemoteAddr)
		writeClose(conn, s.logger, request.Context(), websocket.CloseInternalServerErr, "connection setup failed")
		return
	}

	identity, err := readIdentity(conn, s.logger, request.Context(), request.RemoteAddr)
	if err != nil {
		writeClose(conn, s.logger, request.Context(), websocket.ClosePolicyViolation, "valid identity frame required")
		return
	}
	connectionID := uuid.NewString()
	s.hub.Register(connectionID, socket)
	connection, err := s.operations.Register(request.Context(), identity, connectionID)
	if err != nil {
		s.hub.Remove(connectionID, socket)
		writeClose(conn, s.logger, request.Context(), websocket.CloseInternalServerErr, "connection registration failed")
		return
	}
	defer s.hub.Remove(connection.ConnID, socket)
	s.logger.Info(request.Context(), "websocket connection registered",
		"user_id", identity.UserID, "device_id", identity.DeviceID,
		"ws_node", connection.NodeID, "conn_id", connection.ConnID)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.operations.Unregister(ctx, identity.DeviceID, connection); err != nil {
			// The gateway logs the underlying registry failure with connection context.
		}
		s.logger.Info(ctx, "websocket connection closed",
			"user_id", identity.UserID, "device_id", identity.DeviceID,
			"ws_node", connection.NodeID, "conn_id", connection.ConnID)
	}()
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		s.logger.Error(request.Context(), "could not set websocket read deadline", "error", err,
			"device_id", identity.DeviceID, "conn_id", connection.ConnID)
		return
	}

	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	conn.SetPongHandler(func(string) error {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			s.logger.Error(ctx, "could not extend websocket read deadline", "error", err,
				"device_id", identity.DeviceID, "conn_id", connection.ConnID)
			return err
		}
		registered, err := s.operations.Refresh(ctx, identity.DeviceID, connection)
		if err != nil {
			return err
		}
		if !registered {
			s.logger.Warn(ctx, "websocket connection no longer owns registry entry",
				"device_id", identity.DeviceID, "conn_id", connection.ConnID)
			return errors.New("websocket connection no longer owns registry entry")
		}
		return nil
	})
	go s.pingLoop(ctx, socket)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) && request.Context().Err() == nil {
				s.logger.Warn(request.Context(), "websocket read ended unexpectedly", "error", err,
					"user_id", identity.UserID, "device_id", identity.DeviceID, "conn_id", connection.ConnID)
			} else {
				s.logger.Info(request.Context(), "websocket client disconnected",
					"user_id", identity.UserID, "device_id", identity.DeviceID, "conn_id", connection.ConnID)
			}
			return
		}
		var envelope wsframe.Envelope
		if err := json.Unmarshal(data, &envelope); err != nil {
			s.logger.Warn(ctx, "websocket client sent malformed JSON", "error", err, "device_id", identity.DeviceID)
			s.writeError(ctx, socket, "", "invalid_frame", "frame must be a valid envelope")
			continue
		}
		if envelope.Type != wsframe.TypeMessageSend {
			s.logger.Warn(ctx, "websocket client sent unsupported frame type",
				"frame_type", envelope.Type, "request_id", envelope.ID, "device_id", identity.DeviceID)
			s.writeError(ctx, socket, envelope.ID, "unsupported_type", "unsupported frame type")
			continue
		}
		var payload wsframe.MessageSendPayload
		if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &payload) != nil ||
			strings.TrimSpace(envelope.ID) == "" || strings.TrimSpace(payload.ChatID) == "" ||
			strings.TrimSpace(payload.ClientMsgID) == "" || strings.TrimSpace(payload.Content) == "" {
			s.logger.Warn(ctx, "websocket client sent an invalid message.send frame",
				"request_id", envelope.ID, "device_id", identity.DeviceID)
			s.writeError(ctx, socket, envelope.ID, "invalid_message", "message.send requires id, chatId, clientMsgId, and content")
			continue
		}
		ack, err := s.operations.Send(ctx, identity, domain.SendMessage{
			ChatID: payload.ChatID, ClientMsgID: payload.ClientMsgID, Content: payload.Content,
		})
		if err != nil {
			s.writeError(ctx, socket, envelope.ID, "send_failed", "message was not accepted")
			continue
		}
		s.logger.Info(ctx, "message durably accepted",
			"user_id", identity.UserID, "device_id", identity.DeviceID,
			"chat_id", payload.ChatID, "client_msg_id", ack.ClientMsgID,
			"request_id", envelope.ID, "ts_unix_milli", ack.TimestampMS)
		response := wsframe.Envelope{
			Type: wsframe.TypeMessageAck,
			ID:   envelope.ID,
			TS:   ack.TimestampMS,
			Payload: marshalPayload(ctx, s.logger, wsframe.MessageAckPayload{
				ClientMsgID: ack.ClientMsgID,
				TS:          ack.TimestampMS,
			}),
		}
		err = socket.WriteJSON(response)
		if err != nil {
			s.logger.Error(ctx, "could not write websocket message acknowledgement", "error", err,
				"device_id", identity.DeviceID, "client_msg_id", ack.ClientMsgID, "request_id", envelope.ID)
			return
		}
		s.logger.Info(ctx, "message acknowledgement sent",
			"device_id", identity.DeviceID, "client_msg_id", ack.ClientMsgID,
			"request_id", envelope.ID)
	}
}

func readIdentity(conn *websocket.Conn, logger logging.Logger, ctx context.Context, remoteAddr string) (domain.Identity, error) {
	invalid := func(err error) (domain.Identity, error) {
		logger.Warn(ctx, "websocket connection sent an invalid identity frame", "error", err,
			"remote_addr", remoteAddr)
		return domain.Identity{}, domain.ErrInvalidIdentity
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		return invalid(err)
	}
	var envelope wsframe.Envelope
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Type != wsframe.TypeIdentity {
		if err == nil {
			err = errors.New("first websocket frame must be an identity envelope")
		}
		return invalid(err)
	}
	var payload wsframe.IdentityPayload
	if len(envelope.Payload) == 0 {
		return invalid(errors.New("identity payload is empty"))
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return invalid(err)
	}
	userID, userErr := uuid.Parse(payload.UserID)
	deviceID, deviceErr := uuid.Parse(payload.DeviceID)
	if userErr != nil || deviceErr != nil {
		return invalid(errors.Join(userErr, deviceErr))
	}
	return domain.Identity{UserID: userID.String(), DeviceID: deviceID.String()}, nil
}

func (s *Server) pingLoop(ctx context.Context, conn *safeSocket) {
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			if err != nil {
				s.logger.Warn(ctx, "could not send websocket heartbeat", "error", err)
				return
			}
		}
	}
}

func (s *Server) writeError(ctx context.Context, conn *safeSocket, id, code, message string) {
	response := wsframe.Envelope{Type: "error", ID: id, TS: time.Now().UnixMilli(), Payload: marshalPayload(ctx, s.logger, struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message})}
	err := conn.WriteJSON(response)
	if err != nil {
		s.logger.Warn(ctx, "could not write websocket error frame", "error", err,
			"error_code", code, "request_id", id)
	}
}

func marshalPayload(ctx context.Context, logger logging.Logger, value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		logger.Error(ctx, "could not encode websocket response payload", "error", err)
		return json.RawMessage("null")
	}
	return data
}

func writeClose(conn *websocket.Conn, logger logging.Logger, ctx context.Context, code int, message string) {
	if err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, message), time.Now().Add(time.Second)); err != nil {
		logger.Warn(ctx, "could not send websocket close frame", "error", err, "close_code", code)
	}
}
