package websocket

import (
	"context"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/gorilla/websocket"
	"sync"
	"time"
)

var ErrConnectionGone = port.ErrConnectionGone

type socketWriter interface{ WriteJSON(any) error }
type Hub struct {
	mu      sync.RWMutex
	sockets map[string]socketWriter
}

func NewHub() *Hub { return &Hub{sockets: make(map[string]socketWriter)} }
func (h *Hub) Register(id string, socket socketWriter) {
	h.mu.Lock()
	h.sockets[id] = socket
	h.mu.Unlock()
}
func (h *Hub) Remove(id string, socket socketWriter) {
	h.mu.Lock()
	if h.sockets[id] == socket {
		delete(h.sockets, id)
	}
	h.mu.Unlock()
}
func (h *Hub) Deliver(_ context.Context, id string, envelope wsframe.Envelope) error {
	h.mu.RLock()
	socket := h.sockets[id]
	h.mu.RUnlock()
	if socket == nil {
		return ErrConnectionGone
	}
	return socket.WriteJSON(envelope)
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
func (s *safeSocket) WriteControl(messageType int, data []byte, deadline time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn.WriteControl(messageType, data, deadline)
}
