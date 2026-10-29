package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	wsmetrics "github.com/chat-platform/backend/services/ws-gateway/internal/metrics"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
)

type captureSocket struct {
	value     any
	writeErr  error
	closed    bool
	closeCode int
	values    []wsframe.Envelope
	onWrite   func()
}

func (s *captureSocket) WriteJSON(v any) error {
	s.value = v
	if envelope, ok := v.(wsframe.Envelope); ok {
		s.values = append(s.values, envelope)
	}
	if s.onWrite != nil {
		s.onWrite()
	}
	return s.writeErr
}
func (s *captureSocket) Close() error { s.closed = true; return nil }
func (s *captureSocket) CloseWithCode(code int, _ string) error {
	s.closeCode = code
	s.closed = true
	return nil
}
func TestHubDeliversToRegisteredConnection(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.Register("conn", socket)
	want := wsframe.Envelope{Type: wsframe.TypeMessageRecv, ID: "9"}
	if err := h.Deliver(context.Background(), "conn", want); err != nil {
		t.Fatal(err)
	}
	got, ok := socket.value.(wsframe.Envelope)
	if !ok || got.ID != want.ID {
		t.Fatalf("delivered=%#v", socket.value)
	}
}
func TestHubReportsMissingConnection(t *testing.T) {
	if err := NewHub().Deliver(context.Background(), "gone", wsframe.Envelope{}); err != ErrConnectionGone {
		t.Fatalf("err=%v, want ErrConnectionGone", err)
	}
}

func TestHubCloseAllRequestsReconnectAndRemovesConnections(t *testing.T) {
	h := NewHub()
	first, second := &captureSocket{}, &captureSocket{}
	h.Register("conn-a", first)
	h.Register("conn-b", second)

	h.CloseAll(websocket.CloseGoingAway, "server shutting down; reconnect")

	for name, socket := range map[string]*captureSocket{"first": first, "second": second} {
		if !socket.closed || socket.closeCode != websocket.CloseGoingAway {
			t.Errorf("%s socket close=(closed %t, code %d)", name, socket.closed, socket.closeCode)
		}
	}
	if got := h.ActiveConnectionCount(); got != 0 {
		t.Fatalf("active connections=%d, want 0", got)
	}
}

func TestHubPresenceDeliveryIsScopedToLocalSubscriptions(t *testing.T) {
	h := NewHub()
	first, second := &captureSocket{}, &captureSocket{}
	h.Register("conn-a", first)
	h.Register("conn-b", second)
	if err := h.SubscribePresence("conn-a", "user-b"); err != nil {
		t.Fatal(err)
	}
	if err := h.SubscribePresence("conn-a", "user-b"); err != nil {
		t.Fatal(err)
	}
	h.DeliverPresence(context.Background(), domain.Presence{UserID: "user-b", Status: "online"})
	if len(first.values) != 1 || first.values[0].Type != wsframe.TypePresenceUpdate {
		t.Fatalf("subscribed socket frames=%+v", first.values)
	}
	if len(second.values) != 0 {
		t.Fatalf("unsubscribed socket received presence: %+v", second.values)
	}
	h.UnsubscribePresence("conn-a", "user-b")
	h.DeliverPresence(context.Background(), domain.Presence{UserID: "user-b", Status: "offline"})
	if len(first.values) != 1 {
		t.Fatalf("unsubscribed socket received another update: %+v", first.values)
	}
}

func TestHubConnectionCloseCleansLocalPresenceSubscriptions(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	connection := domain.Connection{NodeID: "node", ConnID: "conn", SessionID: "session"}
	h.RegisterAuthenticated(connection.ConnID, connection.SessionID, "device", connection, socket)
	if err := h.SubscribePresence(connection.ConnID, "user-b"); err != nil {
		t.Fatal(err)
	}
	if h.CloseConnection(connection) == nil {
		t.Fatal("expected connection to close")
	}
	h.DeliverPresence(context.Background(), domain.Presence{UserID: "user-b", Status: "offline"})
	if len(socket.values) != 0 {
		t.Fatalf("closed socket received presence: %+v", socket.values)
	}
}

func TestHubActiveConnectionsGaugeTracksRegistrationAndRemoval(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := wsmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(metrics)
	if err := metrics.RegisterActiveConnections(registry, hub.ActiveConnectionCount); err != nil {
		t.Fatal(err)
	}
	first := &captureSocket{}
	second := &captureSocket{}
	firstConnection := domain.Connection{NodeID: "node", ConnID: "first", SessionID: "session-first"}
	secondConnection := domain.Connection{NodeID: "node", ConnID: "second", SessionID: "session-second"}
	hub.RegisterAuthenticated(firstConnection.ConnID, firstConnection.SessionID, "device-first", firstConnection, first)
	hub.RegisterAuthenticated(secondConnection.ConnID, secondConnection.SessionID, "device-second", secondConnection, second)
	assertMetricValue(t, registry, "ws_active_connections", nil, 2)

	hub.Remove(firstConnection.ConnID, first)
	assertMetricValue(t, registry, "ws_active_connections", nil, 1)
	if hub.CloseConnection(secondConnection) == nil {
		t.Fatal("expected active connection to be revoked")
	}
	assertMetricValue(t, registry, "ws_active_connections", nil, 0)
}

func TestHubCountsActualSocketWritesByPhaseAndResult(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := wsmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(metrics)

	liveSocket := &captureSocket{}
	hub.Register("live", liveSocket)
	if err := hub.Deliver(context.Background(), "live", wsframe.Envelope{Type: wsframe.TypePresenceUpdate}); err != nil {
		t.Fatal(err)
	}
	assertMetricValue(t, registry, "ws_socket_write_attempts_total", map[string]string{"phase": wsmetrics.SocketWriteLive, "result": wsmetrics.SocketWriteSuccess}, 1)

	failedSocket := &captureSocket{writeErr: errors.New("socket write failed")}
	hub.Register("failed", failedSocket)
	if err := hub.Deliver(context.Background(), "failed", wsframe.Envelope{Type: wsframe.TypeMessageRecv}); !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("live write err=%v, want connection-gone", err)
	}
	assertMetricValue(t, registry, "ws_socket_write_attempts_total", map[string]string{"phase": wsmetrics.SocketWriteLive, "result": wsmetrics.SocketWriteError}, 1)

	flushSocket := &captureSocket{}
	hub.RegisterAuthenticated("flush", "session", "device", domain.Connection{}, flushSocket)
	bufferedPayload, err := json.Marshal(wsframe.MessageRecvPayload{OutboxType: wsframe.OutboxTypeDirect, OutboxID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.Deliver(context.Background(), "flush", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: bufferedPayload}); err != nil {
		t.Fatal(err)
	}
	if err := hub.Ready("flush"); err != nil {
		t.Fatal(err)
	}
	assertMetricValue(t, registry, "ws_socket_write_attempts_total", map[string]string{"phase": wsmetrics.SocketWriteSyncFlush, "result": wsmetrics.SocketWriteSuccess}, 1)

	flushFailedSocket := &captureSocket{writeErr: errors.New("flush failed")}
	hub.RegisterAuthenticated("flush-failed", "session", "device", domain.Connection{}, flushFailedSocket)
	if err := hub.Deliver(context.Background(), "flush-failed", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: bufferedPayload}); err != nil {
		t.Fatal(err)
	}
	if err := hub.Ready("flush-failed"); err == nil {
		t.Fatal("Ready succeeded despite socket write failure")
	}
	assertMetricValue(t, registry, "ws_socket_write_attempts_total", map[string]string{"phase": wsmetrics.SocketWriteSyncFlush, "result": wsmetrics.SocketWriteError}, 1)
}

func assertMetricValue(t *testing.T, registry *prometheus.Registry, name string, wantLabels map[string]string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if !sameLabels(labels, wantLabels) {
				continue
			}
			var got float64
			if metric.GetGauge() != nil {
				got = metric.GetGauge().GetValue()
			} else {
				got = metric.GetCounter().GetValue()
			}
			if got != want {
				t.Fatalf("%s labels=%v value=%v, want %v", name, labels, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s labels=%v not found", name, wantLabels)
}

func sameLabels(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}
func TestHubRemoveDoesNotRemoveReplacement(t *testing.T) {
	h := NewHub()
	oldSocket, newSocket := &captureSocket{}, &captureSocket{}
	h.Register("conn", oldSocket)
	h.Register("conn", newSocket)
	h.Remove("conn", oldSocket)
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{ID: "still-here"}); err != nil {
		t.Fatal(err)
	}
	if newSocket.value == nil {
		t.Fatal("replacement socket was removed")
	}
}

func TestHubCloseConnectionIgnoresStaleReplacementEvent(t *testing.T) {
	h := NewHub()
	oldSocket, currentSocket := &captureSocket{}, &captureSocket{}
	old := domain.Connection{NodeID: "node", ConnID: "conn-a", SessionID: "session-a"}
	current := domain.Connection{NodeID: "node", ConnID: "conn-b", SessionID: "session-a"}
	h.RegisterAuthenticated(old.ConnID, old.SessionID, "device-a", old, oldSocket)
	h.RegisterAuthenticated(current.ConnID, current.SessionID, "device-a", current, currentSocket)
	if got := h.CloseConnection(old); got == nil || got.ID != old.ConnID {
		t.Fatalf("targeted revoke result=%+v", got)
	}
	if got := h.CloseConnection(old); got != nil {
		t.Fatalf("duplicate/stale revoke matched: %+v", got)
	}
	if err := h.Ready(current.ConnID); err != nil {
		t.Fatal(err)
	}
	if err := h.Deliver(context.Background(), current.ConnID, wsframe.Envelope{ID: "current"}); err != nil || currentSocket.value == nil {
		t.Fatalf("replacement connection affected: value=%v err=%v", currentSocket.value, err)
	}
	if err := h.Deliver(context.Background(), old.ConnID, wsframe.Envelope{}); !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("revoked connection still registered: %v", err)
	}
}

func TestHubBuffersUntilReadyThenFlushesInOutboxOrder(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.RegisterAuthenticated("conn", "sid", "device", domain.Connection{}, socket)
	for _, id := range []int64{9, 3, 7} {
		payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxType: wsframe.OutboxTypeDirect, OutboxID: id})
		if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	duplicatePayload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxType: wsframe.OutboxTypeDirect, OutboxID: 7})
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: duplicatePayload}); err != nil {
		t.Fatal(err)
	}
	if socket.value != nil {
		t.Fatal("delivery escaped before sync.ready")
	}
	if err := h.Ready("conn"); err != nil {
		t.Fatal(err)
	}
	if got := socket.values; len(got) != 3 {
		t.Fatalf("flushed %d frames, want 3", len(got))
	}
	for i, want := range []int64{3, 7, 9} {
		var payload wsframe.MessageRecvPayload
		if err := json.Unmarshal(socket.values[i].Payload, &payload); err != nil || payload.OutboxID != want {
			t.Fatalf("frame %d payload=%+v err=%v, want outboxId=%d", i, payload, err, want)
		}
	}
	payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxType: wsframe.OutboxTypeDirect, OutboxID: 12})
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if len(socket.values) != 4 {
		t.Fatalf("live delivery count=%d, want 4", len(socket.values))
	}
}

func TestHubBufferDeduplicatesWithinEachOutboxType(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.RegisterAuthenticated("conn", "sid", "device", domain.Connection{}, socket)
	for _, outboxType := range []wsframe.OutboxType{wsframe.OutboxTypeDirect, wsframe.OutboxTypeGroup} {
		for _, id := range []int64{4, 2, 4} {
			payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxType: outboxType, OutboxID: id})
			if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := h.Ready("conn"); err != nil {
		t.Fatal(err)
	}
	if got := len(socket.values); got != 4 {
		t.Fatalf("flushed %d frames, want IDs 2 and 4 from each outbox", got)
	}
	seen := map[wsframe.OutboxType][]int64{}
	for _, envelope := range socket.values {
		var payload wsframe.MessageRecvPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		seen[payload.OutboxType] = append(seen[payload.OutboxType], payload.OutboxID)
	}
	for _, outboxType := range []wsframe.OutboxType{wsframe.OutboxTypeDirect, wsframe.OutboxTypeGroup} {
		if !reflect.DeepEqual(seen[outboxType], []int64{2, 4}) {
			t.Fatalf("%s IDs=%v, want [2 4]", outboxType, seen[outboxType])
		}
	}
}

func TestHubClosesRetryablyWhenSyncBufferOverflows(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.RegisterAuthenticated("conn", "sid", "device", domain.Connection{}, socket)
	for i := int64(1); i <= maxSyncBufferEvents+1; i++ {
		payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxType: wsframe.OutboxTypeDirect, OutboxID: i})
		err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload})
		if i == maxSyncBufferEvents+1 {
			if !errors.Is(err, ErrSyncBufferFull) || !errors.Is(err, ErrConnectionGone) {
				t.Fatalf("overflow err=%v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if !socket.closed || socket.closeCode != websocket.CloseTryAgainLater {
		t.Fatalf("socket closed=%v code=%d, want retryable close", socket.closed, socket.closeCode)
	}
}

func TestHubClosesRetryablyWhenSyncBufferByteLimitIsExceeded(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.RegisterAuthenticated("conn", "sid", "device", domain.Connection{}, socket)
	content := strings.Repeat("x", maxSyncBufferBytes/3)
	for i := int64(1); i <= 3; i++ {
		payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxType: wsframe.OutboxTypeDirect, OutboxID: i, Content: content})
		err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload})
		if i == 3 {
			if !errors.Is(err, ErrSyncBufferFull) {
				t.Fatalf("byte overflow err=%v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if !socket.closed || socket.closeCode != websocket.CloseTryAgainLater {
		t.Fatalf("socket closed=%v code=%d, want retryable close", socket.closed, socket.closeCode)
	}
}

func TestHubWriteFailureRemovesAndClosesFailedSocket(t *testing.T) {
	h := NewHub()
	writeErr := errors.New("broken websocket")
	socket := &captureSocket{writeErr: writeErr}
	h.Register("conn", socket)

	err := h.Deliver(context.Background(), "conn", wsframe.Envelope{})
	if !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("err=%v, want ErrConnectionGone", err)
	}
	if !errors.Is(err, writeErr) {
		t.Fatalf("err=%v, want original write error preserved", err)
	}
	if !socket.closed {
		t.Fatal("failed socket was not closed")
	}
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{}); err != ErrConnectionGone {
		t.Fatalf("delivery after eviction err=%v, want ErrConnectionGone", err)
	}
}

func TestHubWriteFailureCannotRemoveOrCloseReplacement(t *testing.T) {
	h := NewHub()
	oldSocket, replacement := &captureSocket{writeErr: errors.New("broken old websocket")}, &captureSocket{}
	oldSocket.onWrite = func() { h.Register("conn", replacement) }
	h.Register("conn", oldSocket)

	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{}); !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("err=%v, want ErrConnectionGone", err)
	}
	if !oldSocket.closed {
		t.Fatal("failed old socket was not closed")
	}
	if replacement.closed {
		t.Fatal("replacement socket was closed")
	}
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{ID: "replacement"}); err != nil {
		t.Fatalf("replacement delivery failed: %v", err)
	}
	if replacement.value == nil {
		t.Fatal("replacement socket was removed from the hub")
	}
}
