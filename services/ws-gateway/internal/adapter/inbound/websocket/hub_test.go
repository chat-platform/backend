package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/gorilla/websocket"
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
		payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxID: id})
		if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	duplicatePayload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxID: 7})
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
	payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxID: 12})
	if err := h.Deliver(context.Background(), "conn", wsframe.Envelope{Type: wsframe.TypeMessageRecv, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if len(socket.values) != 4 {
		t.Fatalf("live delivery count=%d, want 4", len(socket.values))
	}
}

func TestHubClosesRetryablyWhenSyncBufferOverflows(t *testing.T) {
	h := NewHub()
	socket := &captureSocket{}
	h.RegisterAuthenticated("conn", "sid", "device", domain.Connection{}, socket)
	for i := int64(1); i <= maxSyncBufferEvents+1; i++ {
		payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxID: i})
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
		payload, _ := json.Marshal(wsframe.MessageRecvPayload{OutboxID: i, Content: content})
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
