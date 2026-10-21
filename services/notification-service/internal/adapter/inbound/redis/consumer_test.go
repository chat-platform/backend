package redisadapter

import (
	"testing"
	"time"

	"github.com/chat-platform/backend/services/notification-service/internal/app"
)

func TestAddHintCoalescesByDeviceAndChat(t *testing.T) {
	now := time.Unix(100, 0)
	pending := make(map[string]*pendingBatch)
	owners := make(map[string]string)
	addHint(pending, owners, app.PushHint{DeviceID: "device-a", ChatID: "chat-a"}, "1-0", now, 5*time.Second)
	addHint(pending, owners, app.PushHint{DeviceID: "device-a", ChatID: "chat-a"}, "2-0", now.Add(time.Second), 5*time.Second)
	addHint(pending, owners, app.PushHint{DeviceID: "device-b", ChatID: "chat-a"}, "3-0", now, 5*time.Second)
	addHint(pending, owners, app.PushHint{DeviceID: "device-a", ChatID: "chat-b"}, "4-0", now, 5*time.Second)
	if len(pending) != 3 {
		t.Fatalf("pending batches=%d, want 3", len(pending))
	}
	batch := pending["device-a\x00chat-a"]
	if len(batch.entries) != 2 || !batch.deadline.Equal(now.Add(5*time.Second)) {
		t.Fatalf("coalesced batch=%+v entries=%d", batch, len(batch.entries))
	}
	addHint(pending, owners, app.PushHint{DeviceID: "device-a", ChatID: "chat-a"}, "1-0", now.Add(2*time.Second), 5*time.Second)
	if len(batch.entries) != 2 {
		t.Fatalf("duplicate entry was added: %v", batch.entries)
	}
}
