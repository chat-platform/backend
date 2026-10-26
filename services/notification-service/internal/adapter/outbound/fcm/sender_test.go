package fcm

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"firebase.google.com/go/v4/messaging"
	"github.com/chat-platform/backend/services/notification-service/internal/app"
	servicemetrics "github.com/chat-platform/backend/services/notification-service/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type fakeMessagingClient struct {
	message   *messaging.Message
	messageID string
	err       error
}

func (f *fakeMessagingClient) Send(_ context.Context, message *messaging.Message) (string, error) {
	f.message = message
	return f.messageID, f.err
}

func TestBuildMessageContainsOnlyPushToSyncDataForEveryPlatform(t *testing.T) {
	wantData := map[string]string{"type": "sync", "chatId": "opaque-chat-id"}
	for _, platform := range []string{"ANDROID", "IOS", "WEB"} {
		t.Run(platform, func(t *testing.T) {
			message, err := BuildMessage(app.Push{Token: "fcm-registration-token", Platform: platform, ChatID: "opaque-chat-id"})
			if err != nil {
				t.Fatal(err)
			}
			if message.Token != "fcm-registration-token" || !reflect.DeepEqual(message.Data, wantData) || message.Notification != nil {
				t.Fatalf("message=%+v", message)
			}
			switch platform {
			case "ANDROID":
				if message.Android == nil || message.Android.Priority != "normal" || message.APNS != nil || message.Webpush != nil {
					t.Fatalf("android config=%+v", message.Android)
				}
			case "IOS":
				if message.APNS == nil || message.APNS.Headers["apns-push-type"] != "background" || message.APNS.Headers["apns-priority"] != "5" || message.APNS.Payload == nil || message.APNS.Payload.Aps == nil || !message.APNS.Payload.Aps.ContentAvailable {
					t.Fatalf("APNs config=%+v", message.APNS)
				}
			case "WEB":
				if message.Webpush == nil || !reflect.DeepEqual(message.Webpush.Data, wantData) || message.Webpush.Notification != nil {
					t.Fatalf("Web Push config=%+v", message.Webpush)
				}
			}
		})
	}
}

func TestBuildMessageRejectsInvalidPush(t *testing.T) {
	for _, push := range []app.Push{
		{Token: "token", Platform: "ANDROID"},
		{ChatID: "chat", Platform: "ANDROID"},
		{Token: "token", ChatID: "chat", Platform: "UNKNOWN"},
	} {
		if _, err := BuildMessage(push); err == nil {
			t.Fatalf("BuildMessage(%+v) succeeded, want error", push)
		}
	}
}

func TestSenderMapsUnregisteredTokenAndReturnsTransientErrors(t *testing.T) {
	unregistered := errors.New("unregistered token")
	client := &fakeMessagingClient{messageID: "projects/project/messages/id"}
	registry := prometheus.NewRegistry()
	metrics, err := servicemetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewSender(client, metrics).Send(context.Background(), app.Push{Token: "token", Platform: "ANDROID", ChatID: "chat"}); err != nil {
		t.Fatal(err)
	}
	if client.message == nil || client.message.Notification != nil {
		t.Fatalf("sent message=%+v", client.message)
	}
	client.err = unregistered
	classify := func(err error) bool { return errors.Is(err, unregistered) }
	if err := newSender(client, classify, metrics).Send(context.Background(), app.Push{Token: "token", Platform: "ANDROID", ChatID: "chat"}); !errors.Is(err, app.ErrTokenUnregistered) {
		t.Fatalf("unregistered error=%v", err)
	}
	client.err = errors.New("service unavailable")
	if err := newSender(client, classify, metrics).Send(context.Background(), app.Push{Token: "token", Platform: "ANDROID", ChatID: "chat"}); err == nil || errors.Is(err, app.ErrTokenUnregistered) {
		t.Fatalf("transient error=%v", err)
	}
	for label, want := range map[string]float64{servicemetrics.ProviderAccepted: 1, servicemetrics.ProviderUnregistered: 1, servicemetrics.ProviderError: 1} {
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		got := float64(0)
		for _, family := range families {
			if family.GetName() != "notification_push_attempts_total" {
				continue
			}
			for _, metric := range family.GetMetric() {
				if metric.GetLabel()[0].GetValue() == "fcm" && metric.GetLabel()[1].GetValue() == label {
					got = metric.GetCounter().GetValue()
				}
			}
		}
		if got != want {
			t.Errorf("provider outcome %s count=%v, want %v", label, got, want)
		}
	}
}

func TestNewADCClientFailsWhenCredentialsAreMissing(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", t.TempDir()+"/missing-service-account.json")
	t.Setenv("GOOGLE_CLOUD_PROJECT", "test-project")
	if _, err := NewADCClient(context.Background(), "test-project"); err == nil {
		t.Fatal("expected Firebase initialization to fail without ADC credentials")
	}
}
