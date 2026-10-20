package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/gorilla/websocket"
)

var ErrSyncBufferFull = errors.New("websocket sync buffer limit exceeded")
var ErrConnectionGone = port.ErrConnectionGone

const (
	maxSyncBufferEvents = 1000
	maxSyncBufferBytes  = 10 << 20
)

type socketWriter interface {
	WriteJSON(any) error
	Close() error
	CloseWithCode(int, string) error
}

type socketEntry struct {
	mu                  sync.Mutex
	socket              socketWriter
	sessionID, deviceID string
	connection          domain.Connection
	ready               bool
	failed              bool
	buffer              []bufferedEnvelope
	bufferedBytes       int
	bufferedIDs         map[int64]struct{}
}

type bufferedEnvelope struct {
	envelope wsframe.Envelope
	outboxID int64
}

type Hub struct {
	mu      sync.RWMutex
	entries map[string]*socketEntry
}

type ActiveSocket struct {
	ID, DeviceID string
	Connection   domain.Connection
	Socket       socketWriter
}

func NewHub() *Hub { return &Hub{entries: make(map[string]*socketEntry)} }

func (h *Hub) Register(id string, socket socketWriter) {
	h.RegisterAuthenticated(id, "", "", domain.Connection{}, socket)
	entry := h.entry(id)
	entry.mu.Lock()
	entry.ready = true
	entry.mu.Unlock()
}

func (h *Hub) RegisterAuthenticated(id, sessionID, deviceID string, connection domain.Connection, socket socketWriter) {
	h.mu.Lock()
	h.entries[id] = &socketEntry{
		socket: socket, sessionID: sessionID, deviceID: deviceID,
		connection: connection, bufferedIDs: make(map[int64]struct{}),
	}
	h.mu.Unlock()
}

func (h *Hub) Remove(id string, socket socketWriter) {
	h.mu.Lock()
	if entry := h.entries[id]; entry != nil && entry.socket == socket {
		delete(h.entries, id)
	}
	h.mu.Unlock()
}

func (h *Hub) CloseConnection(connection domain.Connection) *ActiveSocket {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.entries[connection.ConnID]
	if entry == nil || entry.sessionID != connection.SessionID || entry.connection.NodeID != connection.NodeID {
		return nil
	}
	delete(h.entries, connection.ConnID)
	return &ActiveSocket{ID: connection.ConnID, DeviceID: entry.deviceID, Connection: entry.connection, Socket: entry.socket}
}

func (h *Hub) Deliver(_ context.Context, id string, envelope wsframe.Envelope) error {
	entry := h.entry(id)
	if entry == nil {
		return ErrConnectionGone
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode websocket delivery: %w", err)
	}
	var payload wsframe.MessageRecvPayload
	if len(envelope.Payload) > 0 {
		_ = json.Unmarshal(envelope.Payload, &payload)
	}
	entry.mu.Lock()
	if entry.failed {
		entry.mu.Unlock()
		return ErrConnectionGone
	}
	if !entry.ready {
		if envelope.Type == wsframe.TypeMessageRecv && payload.OutboxID <= 0 {
			entry.mu.Unlock()
			return fmt.Errorf("buffered message.recv requires a positive outboxId")
		}
		if payload.OutboxID > 0 {
			if _, exists := entry.bufferedIDs[payload.OutboxID]; exists {
				entry.mu.Unlock()
				return nil
			}
		}
		if len(entry.buffer) >= maxSyncBufferEvents || entry.bufferedBytes+len(encoded) > maxSyncBufferBytes {
			entry.failed = true
			entry.mu.Unlock()
			h.Remove(id, entry.socket)
			_ = entry.socket.CloseWithCode(websocket.CloseTryAgainLater, "sync buffer overflow; reconnect and sync")
			return fmt.Errorf("%w: %w", port.ErrConnectionGone, ErrSyncBufferFull)
		}
		entry.buffer = append(entry.buffer, bufferedEnvelope{envelope: envelope, outboxID: payload.OutboxID})
		entry.bufferedBytes += len(encoded)
		if payload.OutboxID > 0 {
			entry.bufferedIDs[payload.OutboxID] = struct{}{}
		}
		entry.mu.Unlock()
		return nil
	}
	if err := entry.socket.WriteJSON(envelope); err != nil {
		entry.failed = true
		entry.mu.Unlock()
		h.Remove(id, entry.socket)
		_ = entry.socket.Close()
		return fmt.Errorf("%w: websocket write failed: %w", port.ErrConnectionGone, err)
	}
	entry.mu.Unlock()
	return nil
}

func (h *Hub) Ready(id string) error {
	entry := h.entry(id)
	if entry == nil {
		return ErrConnectionGone
	}
	entry.mu.Lock()
	if entry.failed {
		entry.mu.Unlock()
		return ErrConnectionGone
	}
	sort.SliceStable(entry.buffer, func(i, j int) bool {
		return entry.buffer[i].outboxID < entry.buffer[j].outboxID
	})
	for _, item := range entry.buffer {
		if err := entry.socket.WriteJSON(item.envelope); err != nil {
			entry.failed = true
			entry.mu.Unlock()
			h.Remove(id, entry.socket)
			_ = entry.socket.Close()
			return fmt.Errorf("flush websocket sync buffer: %w", err)
		}
	}
	entry.buffer = nil
	entry.bufferedIDs = nil
	entry.bufferedBytes = 0
	entry.ready = true
	entry.mu.Unlock()
	return nil
}

func (h *Hub) entry(id string) *socketEntry {
	h.mu.RLock()
	entry := h.entries[id]
	h.mu.RUnlock()
	return entry
}

type safeSocket struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (s *safeSocket) WriteJSON(value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteJSON(value)
}
func (s *safeSocket) Close() error { return s.conn.Close() }
func (s *safeSocket) CloseWithCode(code int, text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
	closeErr := s.conn.Close()
	if err != nil {
		return err
	}
	return closeErr
}
func (s *safeSocket) WriteControl(messageType int, data []byte, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteControl(messageType, data, deadline)
}
