package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
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
	logger     logging.Logger
	upgrader   websocket.Upgrader
}

func NewServer(operations Operations, logger logging.Logger) *Server {
	return &Server{
		operations: operations,
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
	defer conn.Close()
	conn.SetReadLimit(maxFrameBytes)
	_ = conn.SetReadDeadline(time.Now().Add(identityTimeout))

	identity, err := readIdentity(conn, s.logger, request.Context(), request.RemoteAddr)
	if err != nil {
		writeClose(conn, websocket.ClosePolicyViolation, "valid identity frame required")
		return
	}
	connectionID := uuid.NewString()
	connection, err := s.operations.Register(request.Context(), identity, connectionID)
	if err != nil {
		writeClose(conn, websocket.CloseInternalServerErr, "connection registration failed")
		return
	}
	s.logger.Info(request.Context(), "websocket connection registered",
		"user_id", identity.UserID, "device_id", identity.DeviceID,
		"ws_node", connection.NodeID, "conn_id", connection.ConnID)
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.operations.Unregister(ctx, identity.DeviceID, connection)
		s.logger.Info(ctx, "websocket connection closed",
			"user_id", identity.UserID, "device_id", identity.DeviceID,
			"ws_node", connection.NodeID, "conn_id", connection.ConnID)
	}()

	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	var writeMu sync.Mutex
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
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
	go s.pingLoop(ctx, conn, &writeMu)

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
			s.writeError(conn, &writeMu, "", "invalid_frame", "frame must be a valid envelope")
			continue
		}
		if envelope.Type != wsframe.TypeMessageSend {
			s.logger.Warn(ctx, "websocket client sent unsupported frame type",
				"frame_type", envelope.Type, "request_id", envelope.ID, "device_id", identity.DeviceID)
			s.writeError(conn, &writeMu, envelope.ID, "unsupported_type", "unsupported frame type")
			continue
		}
		var payload wsframe.MessageSendPayload
		if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &payload) != nil ||
			strings.TrimSpace(envelope.ID) == "" || strings.TrimSpace(payload.ChatID) == "" ||
			strings.TrimSpace(payload.ClientMsgID) == "" || strings.TrimSpace(payload.Content) == "" {
			s.logger.Warn(ctx, "websocket client sent an invalid message.send frame",
				"request_id", envelope.ID, "device_id", identity.DeviceID)
			s.writeError(conn, &writeMu, envelope.ID, "invalid_message", "message.send requires id, chatId, clientMsgId, and content")
			continue
		}
		ack, err := s.operations.Send(ctx, identity, domain.SendMessage{
			ChatID: payload.ChatID, ClientMsgID: payload.ClientMsgID, Content: payload.Content,
		})
		if err != nil {
			s.writeError(conn, &writeMu, envelope.ID, "send_failed", "message was not accepted")
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
			Payload: marshalPayload(wsframe.MessageAckPayload{
				ClientMsgID: ack.ClientMsgID,
				TS:          ack.TimestampMS,
			}),
		}
		writeMu.Lock()
		err = conn.WriteJSON(response)
		writeMu.Unlock()
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

func (s *Server) pingLoop(ctx context.Context, conn *websocket.Conn, writeMu *sync.Mutex) {
	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			writeMu.Lock()
			err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			writeMu.Unlock()
			if err != nil {
				s.logger.Warn(ctx, "could not send websocket heartbeat", "error", err)
				return
			}
		}
	}
}

func (s *Server) writeError(conn *websocket.Conn, writeMu *sync.Mutex, id, code, message string) {
	response := wsframe.Envelope{Type: "error", ID: id, TS: time.Now().UnixMilli(), Payload: marshalPayload(struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{code, message})}
	writeMu.Lock()
	err := conn.WriteJSON(response)
	writeMu.Unlock()
	if err != nil {
		s.logger.Warn(context.Background(), "could not write websocket error frame", "error", err,
			"error_code", code, "request_id", id)
	}
}

func marshalPayload(value any) json.RawMessage {
	data, _ := json.Marshal(value)
	return data
}

func writeClose(conn *websocket.Conn, code int, message string) {
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, message), time.Now().Add(time.Second))
}
