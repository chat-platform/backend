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
	wsmetrics "github.com/chat-platform/backend/services/ws-gateway/internal/metrics"
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
	bufferedIDs         map[deliveryKey]struct{}
}

type bufferedEnvelope struct {
	envelope   wsframe.Envelope
	outboxID   int64
	outboxType wsframe.OutboxType
}

type deliveryKey struct {
	outboxType wsframe.OutboxType
	outboxID   int64
}

type Hub struct {
	mu                    sync.RWMutex
	entries               map[string]*socketEntry
	presenceSubscribers   map[string]map[string]struct{}
	presenceSubscriptions map[string]map[string]struct{}
	metrics               *wsmetrics.Metrics
}

type ActiveSocket struct {
	ID, DeviceID string
	Connection   domain.Connection
	Socket       socketWriter
}

func NewHub(metrics ...*wsmetrics.Metrics) *Hub {
	hub := &Hub{
		entries:               make(map[string]*socketEntry),
		presenceSubscribers:   make(map[string]map[string]struct{}),
		presenceSubscriptions: make(map[string]map[string]struct{}),
	}
	if len(metrics) > 0 {
		hub.metrics = metrics[0]
	}
	return hub
}

func (h *Hub) ActiveConnectionCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

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
		connection: connection, bufferedIDs: make(map[deliveryKey]struct{}),
	}
	h.mu.Unlock()
}

func (h *Hub) Remove(id string, socket socketWriter) {
	h.mu.Lock()
	if entry := h.entries[id]; entry != nil && entry.socket == socket {
		delete(h.entries, id)
		h.removePresenceSubscriptionsLocked(id)
	}
	h.mu.Unlock()
}

func (h *Hub) SubscribePresence(connectionID, targetUserID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.entries[connectionID] == nil {
		return ErrConnectionGone
	}
	if h.presenceSubscribers[targetUserID] == nil {
		h.presenceSubscribers[targetUserID] = make(map[string]struct{})
	}
	h.presenceSubscribers[targetUserID][connectionID] = struct{}{}
	if h.presenceSubscriptions[connectionID] == nil {
		h.presenceSubscriptions[connectionID] = make(map[string]struct{})
	}
	h.presenceSubscriptions[connectionID][targetUserID] = struct{}{}
	return nil
}

func (h *Hub) UnsubscribePresence(connectionID, targetUserID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if subscribers := h.presenceSubscribers[targetUserID]; subscribers != nil {
		delete(subscribers, connectionID)
		if len(subscribers) == 0 {
			delete(h.presenceSubscribers, targetUserID)
		}
	}
	if subscriptions := h.presenceSubscriptions[connectionID]; subscriptions != nil {
		delete(subscriptions, targetUserID)
		if len(subscriptions) == 0 {
			delete(h.presenceSubscriptions, connectionID)
		}
	}
}

func (h *Hub) DeliverPresence(ctx context.Context, presence domain.Presence) {
	h.mu.RLock()
	connectionIDs := make([]string, 0, len(h.presenceSubscribers[presence.UserID]))
	for connectionID := range h.presenceSubscribers[presence.UserID] {
		connectionIDs = append(connectionIDs, connectionID)
	}
	h.mu.RUnlock()
	payload, _ := json.Marshal(wsframe.PresencePayload{UserID: presence.UserID, Status: presence.Status, LastSeen: presence.LastSeen})
	envelope := wsframe.Envelope{
		Type: wsframe.TypePresenceUpdate, TS: time.Now().UnixMilli(),
		Payload: payload,
	}
	for _, connectionID := range connectionIDs {
		_ = h.Deliver(ctx, connectionID, envelope)
	}
}

func (h *Hub) removePresenceSubscriptionsLocked(connectionID string) {
	for targetUserID := range h.presenceSubscriptions[connectionID] {
		if subscribers := h.presenceSubscribers[targetUserID]; subscribers != nil {
			delete(subscribers, connectionID)
			if len(subscribers) == 0 {
				delete(h.presenceSubscribers, targetUserID)
			}
		}
	}
	delete(h.presenceSubscriptions, connectionID)
}

func (h *Hub) CloseConnection(connection domain.Connection) *ActiveSocket {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry := h.entries[connection.ConnID]
	if entry == nil || entry.sessionID != connection.SessionID || entry.connection.NodeID != connection.NodeID {
		return nil
	}
	delete(h.entries, connection.ConnID)
	h.removePresenceSubscriptionsLocked(connection.ConnID)
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
		if envelope.Type == wsframe.TypeMessageRecv && (payload.OutboxID <= 0 || (payload.OutboxType != wsframe.OutboxTypeDirect && payload.OutboxType != wsframe.OutboxTypeGroup)) {
			entry.mu.Unlock()
			return fmt.Errorf("buffered message.recv requires a positive outboxId and valid outboxType")
		}
		if payload.OutboxID > 0 {
			key := deliveryKey{outboxType: payload.OutboxType, outboxID: payload.OutboxID}
			if _, exists := entry.bufferedIDs[key]; exists {
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
		entry.buffer = append(entry.buffer, bufferedEnvelope{envelope: envelope, outboxID: payload.OutboxID, outboxType: payload.OutboxType})
		entry.bufferedBytes += len(encoded)
		if payload.OutboxID > 0 {
			entry.bufferedIDs[deliveryKey{outboxType: payload.OutboxType, outboxID: payload.OutboxID}] = struct{}{}
		}
		entry.mu.Unlock()
		return nil
	}
	err = entry.socket.WriteJSON(envelope)
	h.metrics.RecordSocketWrite(wsmetrics.SocketWriteLive, err)
	if err != nil {
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
		if entry.buffer[i].outboxType != entry.buffer[j].outboxType {
			return entry.buffer[i].outboxType < entry.buffer[j].outboxType
		}
		return entry.buffer[i].outboxID < entry.buffer[j].outboxID
	})
	for _, item := range entry.buffer {
		err := entry.socket.WriteJSON(item.envelope)
		h.metrics.RecordSocketWrite(wsmetrics.SocketWriteSyncFlush, err)
		if err != nil {
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
