package websocket

import (
	"context"
	"errors"
	"testing"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
)

type captureSocket struct {
	value    any
	writeErr error
	closed   bool
	onWrite  func()
}

func (s *captureSocket) WriteJSON(v any) error {
	s.value = v
	if s.onWrite != nil {
		s.onWrite()
	}
	return s.writeErr
}
func (s *captureSocket) Close() error { s.closed = true; return nil }
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

func TestHubCloseSessionReturnsOnlyMatchingConnections(t *testing.T) {
	h := NewHub()
	closed, retained := &captureSocket{}, &captureSocket{}
	h.RegisterAuthenticated("conn-a", "session-a", "device-a", domain.Connection{NodeID: "node", ConnID: "conn-a", SessionID: "session-a"}, closed)
	h.RegisterAuthenticated("conn-b", "session-b", "device-b", domain.Connection{NodeID: "node", ConnID: "conn-b", SessionID: "session-b"}, retained)
	active := h.CloseSession("session-a")
	if len(active) != 1 || active[0].ID != "conn-a" || active[0].DeviceID != "device-a" {
		t.Fatalf("closed connections=%+v", active)
	}
	if err := h.Deliver(context.Background(), "conn-a", wsframe.Envelope{}); !errors.Is(err, ErrConnectionGone) {
		t.Fatalf("revoked connection still registered: %v", err)
	}
	if err := h.Deliver(context.Background(), "conn-b", wsframe.Envelope{}); err != nil {
		t.Fatalf("unrelated connection removed: %v", err)
	}
	if retained.value == nil {
		t.Fatal("unrelated socket did not receive delivery")
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
