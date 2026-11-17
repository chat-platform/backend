package redisadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const callSignalChannel = "ws:call:signals"
const activeCallTTL = 4 * time.Hour
const callReconnectGrace = 10 * time.Second

var createCallScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 or redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[2])
redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[2])
return 1
`)

var acceptCallScript = redis.NewScript(`
local raw = redis.call('GET', KEYS[1]); if not raw then return 0 end
local call = cjson.decode(raw)
if call.status ~= 'ringing' then return 0 end
if call.callee ~= ARGV[1] then return -1 end
if call.accepted_device and call.accepted_device ~= ARGV[2] then return -2 end
call.accepted_device = ARGV[2]; call.status = 'active'
redis.call('SET', KEYS[1], cjson.encode(call), 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3]); redis.call('EXPIRE', KEYS[3], ARGV[3])
return 1
`)

type callState struct {
	ID             string `json:"id"`
	ChatID         string `json:"chat_id"`
	Caller         string `json:"caller"`
	CallerDevice   string `json:"caller_device"`
	Callee         string `json:"callee"`
	Mode           string `json:"mode"`
	Status         string `json:"status"`
	AcceptedDevice string `json:"accepted_device,omitempty"`
}

type CallPeerResolver interface {
	ResolveDirectChatPeer(context.Context, string, string) (string, error)
}

type CallCoordinator struct {
	client *redis.Client
	peers  CallPeerResolver
}

func NewCallCoordinator(client *redis.Client, peers CallPeerResolver) *CallCoordinator {
	return &CallCoordinator{client: client, peers: peers}
}

func (c *CallCoordinator) Handle(ctx context.Context, identity domain.Identity, eventType string, raw []byte) error {
	var payload wsframe.CallPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return errors.New("invalid call payload")
	}
	if _, err := uuid.Parse(payload.CallID); err != nil {
		return errors.New("invalid call id")
	}
	if eventType == wsframe.TypeCallInvite {
		if _, err := uuid.Parse(payload.ChatID); err != nil {
			return errors.New("invalid call chat")
		}
		if payload.CallMode != "audio" && payload.CallMode != "video" {
			return errors.New("invalid call mode")
		}
		peer, err := c.peers.ResolveDirectChatPeer(ctx, payload.ChatID, identity.UserID)
		if err != nil {
			return err
		}
		state := callState{ID: payload.CallID, ChatID: payload.ChatID, Caller: identity.UserID, CallerDevice: identity.DeviceID, Callee: peer, Mode: payload.CallMode, Status: "ringing"}
		encoded, _ := json.Marshal(state)
		result, err := createCallScript.Run(ctx, c.client, []string{callUserKey(identity.UserID), callUserKey(peer), callStateKey(payload.CallID)}, payload.CallID, 45, string(encoded)).Int()
		if err != nil {
			return fmt.Errorf("reserve call: %w", err)
		}
		if result == 0 {
			return c.publish(ctx, identity.UserID, wsframe.Envelope{Type: wsframe.TypeCallBusy, TS: time.Now().UnixMilli(), Payload: marshalCall(wsframe.CallPayload{CallID: payload.CallID})})
		}
		payload.CallerUserID, payload.CallerDeviceID = identity.UserID, identity.DeviceID
		if err := c.publish(ctx, peer, wsframe.Envelope{Type: eventType, ID: payload.CallID, TS: time.Now().UnixMilli(), Payload: marshalCall(payload)}); err != nil {
			return err
		}
		// Push is a wake-up hint; Redis call state remains the source of truth and expires quickly.
		if !c.userHasLiveSocket(ctx, peer) {
			_ = c.client.XAdd(ctx, &redis.XAddArgs{Stream: "notif:stream", Values: map[string]any{
				"notification_type": "call", "user_id": peer, "call_id": payload.CallID,
				"chat_id": payload.ChatID, "caller_id": identity.UserID,
			}}).Err()
		}
		return nil
	}

	var state callState
	stored, err := c.client.Get(ctx, callStateKey(payload.CallID)).Result()
	if err == redis.Nil {
		return errors.New("call no longer exists")
	}
	if err != nil {
		return fmt.Errorf("read call state: %w", err)
	}
	if err := json.Unmarshal([]byte(stored), &state); err != nil {
		return errors.New("call state is invalid")
	}
	if eventType == wsframe.TypeCallResume && identity.UserID == state.Callee && state.Status == "ringing" {
		payload.ChatID, payload.CallerUserID, payload.CallerDeviceID, payload.CallMode = state.ChatID, state.Caller, state.CallerDevice, state.Mode
		return c.publish(ctx, identity.UserID, wsframe.Envelope{Type: wsframe.TypeCallInvite, ID: payload.CallID, TS: time.Now().UnixMilli(), Payload: marshalCall(payload)})
	}
	var target string
	if identity.UserID == state.Caller {
		target = state.Callee
	} else if identity.UserID == state.Callee {
		target = state.Caller
	} else {
		return errors.New("user is not a call participant")
	}
	if identity.UserID == state.Caller && identity.DeviceID != state.CallerDevice {
		return errors.New("call belongs to another device")
	}
	if eventType == wsframe.TypeCallAccept {
		result, err := acceptCallScript.Run(ctx, c.client, []string{callStateKey(payload.CallID), callUserKey(state.Caller), callUserKey(state.Callee)}, identity.UserID, identity.DeviceID, int(activeCallTTL.Seconds())).Int()
		if err != nil {
			return fmt.Errorf("accept call: %w", err)
		}
		if result != 1 {
			return errors.New("call is no longer available to accept")
		}
		state.AcceptedDevice, state.Status = identity.DeviceID, "active"
		payload.CallerUserID, payload.CallerDeviceID = state.Caller, identity.DeviceID
		payload.AcceptedDeviceID = identity.DeviceID
		if err := c.publish(ctx, state.Callee, wsframe.Envelope{Type: wsframe.TypeCallAcceptedElsewhere, ID: payload.CallID, TS: time.Now().UnixMilli(), Payload: marshalCall(payload)}); err != nil {
			return err
		}
	} else if identity.UserID == state.Callee && state.AcceptedDevice != "" && identity.DeviceID != state.AcceptedDevice && eventType != wsframe.TypeCallReject {
		return errors.New("call was answered on another device")
	}
	if eventType == wsframe.TypeCallReject && identity.UserID != state.Callee {
		return errors.New("only the callee can reject a call")
	}
	if eventType == wsframe.TypeCallReject && state.Status != "ringing" {
		return errors.New("call is no longer ringing")
	}
	if eventType == wsframe.TypeCallCancel && identity.UserID != state.Caller {
		return errors.New("only the caller can cancel a call")
	}
	if eventType == wsframe.TypeCallCancel && state.Status != "ringing" {
		return errors.New("active calls must be ended")
	}
	if eventType == wsframe.TypeCallOffer && identity.UserID != state.Caller {
		return errors.New("only the caller can send an offer")
	}
	if eventType == wsframe.TypeCallAnswer && identity.UserID != state.Callee {
		return errors.New("only the callee can send an answer")
	}
	if (eventType == wsframe.TypeCallOffer || eventType == wsframe.TypeCallAnswer || eventType == wsframe.TypeCallICE) && state.Status != "active" {
		return errors.New("call is not active")
	}
	if eventType == wsframe.TypeCallEnd || eventType == wsframe.TypeCallReject || eventType == wsframe.TypeCallCancel || eventType == wsframe.TypeCallFailed || eventType == wsframe.TypeCallBusy {
		state.Status = "ended"
		encoded, _ := json.Marshal(state)
		_ = c.client.Set(ctx, callStateKey(state.ID), encoded, 15*time.Second).Err()
		_ = c.client.Del(ctx, callUserKey(state.Caller), callUserKey(state.Callee)).Err()
		if err := c.publish(ctx, identity.UserID, wsframe.Envelope{Type: wsframe.TypeCallDismissedElsewhere, ID: payload.CallID, TS: time.Now().UnixMilli(), Payload: marshalCall(wsframe.CallPayload{CallID: payload.CallID})}); err != nil {
			return err
		}
	}
	if eventType == wsframe.TypeCallResume {
		for _, key := range []string{callStateKey(state.ID), callUserKey(state.Caller), callUserKey(state.Callee)} {
			if err := c.client.Expire(ctx, key, activeCallTTL).Err(); err != nil {
				return fmt.Errorf("refresh resumed call: %w", err)
			}
		}
	}
	if eventType == wsframe.TypeCallOffer || eventType == wsframe.TypeCallAnswer || eventType == wsframe.TypeCallICE {
		for _, key := range []string{callStateKey(state.ID), callUserKey(state.Caller), callUserKey(state.Callee)} {
			_ = c.client.Expire(ctx, key, activeCallTTL).Err()
		}
	}
	// ICE and SDP exist only in this publish operation; call state contains no signaling payload.
	if strings.TrimSpace(payload.CallID) == "" {
		return errors.New("call id is required")
	}
	return c.publish(ctx, target, wsframe.Envelope{Type: eventType, ID: payload.CallID, TS: time.Now().UnixMilli(), Payload: marshalCall(payload)})
}

func (c *CallCoordinator) userHasLiveSocket(ctx context.Context, userID string) bool {
	connections, err := c.client.SMembers(ctx, presenceUserKey(userID)).Result()
	if err != nil {
		return false
	}
	for _, id := range connections {
		count, err := c.client.Exists(ctx, "presence:conn:"+userID+":"+id).Result()
		if err == nil && count > 0 {
			return true
		}
	}
	return false
}

func (c *CallCoordinator) Disconnected(ctx context.Context, identity domain.Identity) error {
	callID, err := c.client.Get(ctx, callUserKey(identity.UserID)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("find call on disconnect: %w", err)
	}
	raw, err := c.client.Get(ctx, callStateKey(callID)).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read call on disconnect: %w", err)
	}
	var state callState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return err
	}
	if (identity.UserID == state.Caller && identity.DeviceID != state.CallerDevice) || (identity.UserID == state.Callee && state.AcceptedDevice != "" && identity.DeviceID != state.AcceptedDevice) {
		return nil
	}
	for _, key := range []string{callStateKey(callID), callUserKey(state.Caller), callUserKey(state.Callee)} {
		if err := c.client.Expire(ctx, key, callReconnectGrace).Err(); err != nil {
			return err
		}
	}
	peer := state.Caller
	if identity.UserID == state.Caller {
		peer = state.Callee
	}
	return c.publish(ctx, peer, wsframe.Envelope{Type: wsframe.TypeCallReconnecting, TS: time.Now().UnixMilli(), Payload: marshalCall(wsframe.CallPayload{CallID: callID})})
}

func (c *CallCoordinator) publish(ctx context.Context, userID string, envelope wsframe.Envelope) error {
	encoded, err := json.Marshal(struct {
		UserID   string           `json:"userId"`
		Envelope wsframe.Envelope `json:"envelope"`
	}{userID, envelope})
	if err != nil {
		return err
	}
	if err := c.client.Publish(ctx, callSignalChannel, encoded).Err(); err != nil {
		return fmt.Errorf("publish call signal: %w", err)
	}
	return nil
}

func marshalCall(payload wsframe.CallPayload) json.RawMessage {
	encoded, _ := json.Marshal(payload)
	return encoded
}
func callStateKey(id string) string { return "call:state:" + id }
func callUserKey(id string) string  { return "call:user:" + id }
