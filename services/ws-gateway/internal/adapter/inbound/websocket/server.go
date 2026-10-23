package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	authTimeout    = 10 * time.Second
	heartbeatEvery = 19 * time.Second
	readTimeout    = 60 * time.Second
	maxFrameBytes  = 1 << 20
)

type Operations interface {
	Authenticate(context.Context, string) (domain.Identity, error)
	IsSessionActive(context.Context, string) (bool, error)
	Register(context.Context, domain.Identity, string) (domain.Connection, error)
	Refresh(context.Context, string, string, domain.Connection) (bool, error)
	Unregister(context.Context, string, string, domain.Connection) error
	Send(context.Context, domain.Identity, domain.SendMessage) (domain.MessageAck, error)
	MarkDelivered(context.Context, domain.Identity, string, int64) error
	MarkRead(context.Context, domain.Identity, string, int64, int64) error
	SubscribePresence(context.Context, string, string, string) error
	UnsubscribePresence(context.Context, string, string) error
	GetPresence(context.Context, string, string) (domain.Presence, error)
	PresenceExpired(context.Context, string) error
}

type Server struct {
	operations        Operations
	hub               *Hub
	logger            logging.Logger
	upgrader          websocket.Upgrader
	heartbeatInterval time.Duration
}

func NewServer(operations Operations, logger logging.Logger, hub *Hub) *Server {
	return &Server{
		operations:        operations,
		hub:               hub,
		logger:            logger,
		heartbeatInterval: heartbeatEvery,
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
	if err := conn.SetReadDeadline(time.Now().Add(authTimeout)); err != nil {
		s.logger.Error(request.Context(), "could not set websocket auth read deadline", "error", err,
			"remote_addr", request.RemoteAddr)
		writeClose(conn, s.logger, request.Context(), websocket.CloseInternalServerErr, "connection setup failed")
		return
	}

	token, err := readAuthToken(conn, request.Context())
	if err != nil {
		if request.Context().Err() == nil && !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			s.logOperationFailure(request.Context(), "read websocket auth frame", err, "remote_addr", request.RemoteAddr)
		}
		writeClose(conn, s.logger, request.Context(), websocket.ClosePolicyViolation, "valid auth frame required")
		return
	}
	identity, err := s.operations.Authenticate(request.Context(), token)
	if err != nil {
		s.logOperationFailure(request.Context(), "authenticate websocket", err, "remote_addr", request.RemoteAddr)
		writeClose(conn, s.logger, request.Context(), websocket.ClosePolicyViolation, "valid session token required")
		return
	}
	connectionID := uuid.NewString()
	connection, err := s.operations.Register(request.Context(), identity, connectionID)
	if err != nil {
		s.logOperationFailure(request.Context(), "register websocket connection", err,
			"user_id", identity.UserID, "device_id", identity.DeviceID, "conn_id", connectionID)
		writeClose(conn, s.logger, request.Context(), websocket.CloseInternalServerErr, "connection registration failed")
		return
	}
	s.hub.RegisterAuthenticated(connectionID, identity.SessionID, identity.DeviceID, connection, socket)
	defer func() {
		s.hub.Remove(connection.ConnID, socket)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.operations.Unregister(ctx, identity.DeviceID, identity.UserID, connection); err != nil {
			s.logOperationFailure(ctx, "unregister websocket connection", err,
				"user_id", identity.UserID, "device_id", identity.DeviceID,
				"ws_node", connection.NodeID, "conn_id", connection.ConnID)
		}
		s.logger.Info(ctx, "websocket connection closed",
			"user_id", identity.UserID, "device_id", identity.DeviceID,
			"ws_node", connection.NodeID, "conn_id", connection.ConnID)
	}()
	active, err := s.operations.IsSessionActive(request.Context(), token)
	if err != nil || !active {
		if err != nil {
			s.logOperationFailure(request.Context(), "revalidate websocket session", err,
				"user_id", identity.UserID, "device_id", identity.DeviceID,
				"session_id", identity.SessionID, "conn_id", connection.ConnID)
		} else {
			s.logger.Debug(request.Context(), "websocket session is no longer active",
				"user_id", identity.UserID, "device_id", identity.DeviceID,
				"session_id", identity.SessionID, "conn_id", connection.ConnID)
		}
		writeClose(conn, s.logger, request.Context(), websocket.ClosePolicyViolation, "session is no longer active")
		return
	}
	if err := socket.WriteJSON(wsframe.Envelope{Type: wsframe.TypeAuthOK, TS: time.Now().UnixMilli(), Payload: json.RawMessage(`{}`)}); err != nil {
		s.logger.Warn(request.Context(), "could not write websocket auth acknowledgement", "error", err, "session_id", identity.SessionID)
		return
	}
	s.logger.Info(request.Context(), "websocket connection registered",
		"user_id", identity.UserID, "device_id", identity.DeviceID,
		"ws_node", connection.NodeID, "conn_id", connection.ConnID)
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		s.logger.Error(request.Context(), "could not set websocket read deadline", "error", err,
			"device_id", identity.DeviceID, "conn_id", connection.ConnID)
		return
	}

	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	conn.SetPongHandler(func(string) error {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("extend websocket read deadline: %w", err)
		}
		registered, err := s.operations.Refresh(ctx, identity.DeviceID, identity.UserID, connection)
		if err != nil {
			return err
		}
		if !registered {
			return domain.ErrConnectionSuperseded
		}
		active, err := s.operations.IsSessionActive(ctx, token)
		if err != nil {
			return err
		}
		if !active {
			return fmt.Errorf("%w: websocket session has been revoked", domain.ErrInvalidIdentity)
		}
		return nil
	})
	go s.pingLoop(ctx, socket)

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) && request.Context().Err() == nil {
				s.logOperationFailure(request.Context(), "read websocket frame", err,
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
		if envelope.Type == wsframe.TypeSyncReady {
			if err := s.hub.Ready(connection.ConnID); err != nil {
				s.logger.Warn(ctx, "could not complete websocket sync handoff", "error", err, "device_id", identity.DeviceID, "conn_id", connection.ConnID)
				writeClose(conn, s.logger, ctx, websocket.CloseTryAgainLater, "sync handoff failed; reconnect and sync")
				return
			}
			continue
		}
		if envelope.Type == wsframe.TypeReceiptDelivered {
			var receipt wsframe.DeliveryReceiptPayload
			if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &receipt) != nil || receipt.OutboxID <= 0 ||
				(receipt.OutboxType != wsframe.OutboxTypeDirect && receipt.OutboxType != wsframe.OutboxTypeGroup) {
				s.logger.Warn(ctx, "websocket client sent an invalid delivered receipt",
					"request_id", envelope.ID, "device_id", identity.DeviceID)
				s.writeError(ctx, socket, envelope.ID, "invalid_receipt", "receipt.delivered requires a valid outboxType and positive outboxId")
				continue
			}
			if err := s.operations.MarkDelivered(ctx, identity, string(receipt.OutboxType), receipt.OutboxID); err != nil {
				s.logOperationFailure(ctx, "record delivered receipt", err,
					"request_id", envelope.ID, "user_id", identity.UserID,
					"device_id", identity.DeviceID, "outbox_type", receipt.OutboxType,
					"outbox_id", receipt.OutboxID)
				s.writeError(ctx, socket, envelope.ID, "receipt_failed", "delivery receipt could not be recorded")
				continue
			}
			continue
		}
		if envelope.Type == wsframe.TypeReceiptRead {
			var receipt wsframe.ReadReceiptPayload
			decodeErr := json.Unmarshal(envelope.Payload, &receipt)
			if len(envelope.Payload) == 0 || decodeErr != nil ||
				(receipt.OutboxType != wsframe.OutboxTypeDirect && receipt.OutboxType != wsframe.OutboxTypeGroup) ||
				receipt.OutboxID <= 0 || receipt.ReadAt <= 0 {
				s.logger.Warn(ctx, "websocket client sent an invalid read receipt",
					"request_id", envelope.ID, "device_id", identity.DeviceID)
				s.writeError(ctx, socket, envelope.ID, "invalid_receipt", "receipt.read requires a valid outboxType, positive outboxId, and positive readAt")
				continue
			}
			if err := s.operations.MarkRead(ctx, identity, string(receipt.OutboxType), receipt.OutboxID, receipt.ReadAt); err != nil {
				s.logOperationFailure(ctx, "record read receipt", err,
					"request_id", envelope.ID, "user_id", identity.UserID,
					"device_id", identity.DeviceID, "outbox_type", receipt.OutboxType,
					"outbox_id", receipt.OutboxID)
				s.writeError(ctx, socket, envelope.ID, "receipt_failed", "read receipt could not be recorded")
			}
			continue
		}
		if envelope.Type == wsframe.TypePresenceSubscribe {
			var request wsframe.PresenceTargetPayload
			if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &request) != nil || strings.TrimSpace(request.TargetUserID) == "" {
				s.writeError(ctx, socket, envelope.ID, "invalid_presence_request", "presence.subscribe requires targetUserId")
				continue
			}
			if err := s.operations.SubscribePresence(ctx, token, connection.ConnID, request.TargetUserID); err != nil {
				s.logOperationFailure(ctx, "subscribe to presence", err, "target_user_id", request.TargetUserID, "conn_id", connection.ConnID)
				s.writeError(ctx, socket, envelope.ID, "presence_forbidden", "presence is unavailable for this user")
				continue
			}
			if err := s.hub.SubscribePresence(connection.ConnID, request.TargetUserID); err != nil {
				_ = s.operations.UnsubscribePresence(ctx, connection.ConnID, request.TargetUserID)
				s.writeError(ctx, socket, envelope.ID, "connection_closed", "websocket connection is no longer active")
				continue
			}
			presence, err := s.operations.GetPresence(ctx, token, request.TargetUserID)
			if err != nil {
				s.hub.UnsubscribePresence(connection.ConnID, request.TargetUserID)
				_ = s.operations.UnsubscribePresence(ctx, connection.ConnID, request.TargetUserID)
				s.logOperationFailure(ctx, "read presence snapshot", err, "target_user_id", request.TargetUserID)
				s.writeError(ctx, socket, envelope.ID, "presence_unavailable", "presence snapshot is unavailable")
				continue
			}
			payload := marshalPayload(ctx, s.logger, wsframe.PresencePayload{UserID: presence.UserID, Status: presence.Status, LastSeen: presence.LastSeen})
			if err := socket.WriteJSON(wsframe.Envelope{Type: wsframe.TypePresenceSnapshot, ID: envelope.ID, TS: time.Now().UnixMilli(), Payload: payload}); err != nil {
				s.logOperationFailure(ctx, "write presence snapshot", err, "target_user_id", request.TargetUserID)
				return
			}
			continue
		}
		if envelope.Type == wsframe.TypePresenceUnsubscribe {
			var request wsframe.PresenceTargetPayload
			if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &request) != nil || strings.TrimSpace(request.TargetUserID) == "" {
				s.writeError(ctx, socket, envelope.ID, "invalid_presence_request", "presence.unsubscribe requires targetUserId")
				continue
			}
			s.hub.UnsubscribePresence(connection.ConnID, request.TargetUserID)
			if err := s.operations.UnsubscribePresence(ctx, connection.ConnID, request.TargetUserID); err != nil {
				s.writeError(ctx, socket, envelope.ID, "presence_unsubscribe_failed", "presence subscription could not be removed")
				continue
			}
			payload := marshalPayload(ctx, s.logger, wsframe.PresenceTargetPayload{TargetUserID: request.TargetUserID})
			_ = socket.WriteJSON(wsframe.Envelope{Type: "presence.unsubscribed", ID: envelope.ID, TS: time.Now().UnixMilli(), Payload: payload})
			continue
		}
		if envelope.Type == wsframe.TypePresenceGet {
			var request wsframe.PresenceTargetPayload
			if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &request) != nil || strings.TrimSpace(request.TargetUserID) == "" {
				s.writeError(ctx, socket, envelope.ID, "invalid_presence_request", "presence.get requires targetUserId")
				continue
			}
			presence, err := s.operations.GetPresence(ctx, token, request.TargetUserID)
			if err != nil {
				s.logOperationFailure(ctx, "query presence", err, "target_user_id", request.TargetUserID)
				s.writeError(ctx, socket, envelope.ID, "presence_unavailable", "presence is unavailable for this user")
				continue
			}
			payload := marshalPayload(ctx, s.logger, wsframe.PresencePayload{UserID: presence.UserID, Status: presence.Status, LastSeen: presence.LastSeen})
			_ = socket.WriteJSON(wsframe.Envelope{Type: wsframe.TypePresenceSnapshot, ID: envelope.ID, TS: time.Now().UnixMilli(), Payload: payload})
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
			s.logOperationFailure(ctx, "send message", err,
				"request_id", envelope.ID, "user_id", identity.UserID,
				"device_id", identity.DeviceID, "chat_id", payload.ChatID,
				"client_msg_id", payload.ClientMsgID)
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

func readAuthToken(conn *websocket.Conn, ctx context.Context) (string, error) {
	invalid := func(err error) (string, error) {
		return "", fmt.Errorf("%w: %w", domain.ErrMalformedAuthFrame, err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		if ctx.Err() != nil || websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			return "", err
		}
		return invalid(err)
	}
	var envelope wsframe.Envelope
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Type != wsframe.TypeAuth {
		if err == nil {
			err = errors.New("first websocket frame must be an auth envelope")
		}
		return invalid(err)
	}
	var payload wsframe.AuthPayload
	if len(envelope.Payload) == 0 {
		return invalid(errors.New("auth payload is empty"))
	}
	if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
		return invalid(err)
	}
	if strings.TrimSpace(payload.Token) == "" {
		return invalid(errors.New("auth token is empty"))
	}
	return payload.Token, nil
}

func (s *Server) logOperationFailure(ctx context.Context, operation string, err error, fields ...any) {
	if err == nil {
		return
	}
	args := []any{"operation", operation, "error", err}
	if code := status.Code(err); code != codes.Unknown {
		args = append(args, "grpc_code", code.String())
	}
	args = append(args, fields...)
	switch {
	case errors.Is(err, domain.ErrMalformedAuthFrame):
		s.logger.Warn(ctx, "websocket client request rejected", args...)
	case errors.Is(err, domain.ErrInvalidIdentity), errors.Is(err, context.Canceled):
		s.logger.Debug(ctx, "websocket operation rejected or canceled", args...)
	case errors.Is(err, domain.ErrInvalidMessage), errors.Is(err, domain.ErrConnectionSuperseded),
		status.Code(err) == codes.InvalidArgument, status.Code(err) == codes.PermissionDenied,
		status.Code(err) == codes.NotFound, status.Code(err) == codes.FailedPrecondition:
		s.logger.Warn(ctx, "websocket operation rejected", args...)
	default:
		s.logger.Error(ctx, "websocket operation failed", args...)
	}
}

func (s *Server) RevokeConnection(ctx context.Context, connection domain.Connection) {
	active := s.hub.CloseConnection(connection)
	if active == nil {
		return
	}
	if err := active.Socket.Close(); err != nil {
		s.logger.Warn(ctx, "could not close revoked websocket", "error", err, "session_id", connection.SessionID, "conn_id", active.ID)
	}
	// ServeHTTP owns registry cleanup and logs any failure once after Close unblocks its read loop.
}

func (s *Server) pingLoop(ctx context.Context, conn *safeSocket) {
	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			if err != nil {
				if ctx.Err() == nil {
					_ = conn.Close()
				}
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
