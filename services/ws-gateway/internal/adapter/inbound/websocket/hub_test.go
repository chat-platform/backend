package websocket

import (
	"context"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"testing"
)

type captureSocket struct{ value any }

func (s *captureSocket) WriteJSON(v any) error { s.value = v; return nil }
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
