package redisadapter

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type callPeerStub struct {
	peer string
	err  error
}

func (s callPeerStub) ResolveDirectChatPeer(context.Context, string, string) (string, error) {
	return s.peer, s.err
}

func TestCallCoordinatorReservesParticipantsAndKeepsSDPOutOfState(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run Redis call coordination tests")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis is unavailable: %v", err)
	}
	caller, callee, callerDevice, calleeDevice := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	callID := uuid.NewString()
	defer client.Del(ctx, callStateKey(callID), callUserKey(caller), callUserKey(callee))
	coordinator := NewCallCoordinator(client, callPeerStub{peer: callee})
	if err := coordinator.Handle(ctx, domain.Identity{UserID: caller, DeviceID: callerDevice}, wsframe.TypeCallInvite, mustJSON(t, wsframe.CallPayload{CallID: callID, ChatID: uuid.NewString(), CallMode: "video"})); err != nil {
		t.Fatal(err)
	}
	if got, err := client.Get(ctx, callUserKey(caller)).Result(); err != nil || got != callID {
		t.Fatalf("caller reservation=%q err=%v", got, err)
	}
	if err := coordinator.Handle(ctx, domain.Identity{UserID: caller, DeviceID: callerDevice}, wsframe.TypeCallOffer, mustJSON(t, wsframe.CallPayload{CallID: callID})); err == nil {
		t.Fatal("offer before acceptance was allowed")
	}
	if err := coordinator.Handle(ctx, domain.Identity{UserID: callee, DeviceID: calleeDevice}, wsframe.TypeCallAccept, mustJSON(t, wsframe.CallPayload{CallID: callID})); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Handle(ctx, domain.Identity{UserID: callee, DeviceID: uuid.NewString()}, wsframe.TypeCallAccept, mustJSON(t, wsframe.CallPayload{CallID: callID})); err == nil {
		t.Fatal("second device accepted an active call")
	}
	secretSDP := `{"sdp":"private-credential"}`
	if err := coordinator.Handle(ctx, domain.Identity{UserID: caller, DeviceID: callerDevice}, wsframe.TypeCallOffer, mustJSON(t, wsframe.CallPayload{CallID: callID, SDP: json.RawMessage(secretSDP)})); err != nil {
		t.Fatal(err)
	}
	state, err := client.Get(ctx, callStateKey(callID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(state, "private-credential") || strings.Contains(state, "sdp") {
		t.Fatalf("media signaling was persisted: %s", state)
	}
	if ttl, err := client.TTL(ctx, callStateKey(callID)).Result(); err != nil || ttl < time.Hour {
		t.Fatalf("active call TTL=%v err=%v", ttl, err)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
