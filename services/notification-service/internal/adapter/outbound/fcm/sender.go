package fcm

import (
	"context"
	"errors"
	"fmt"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"github.com/chat-platform/backend/services/notification-service/internal/app"
	servicemetrics "github.com/chat-platform/backend/services/notification-service/internal/metrics"
)

type MessagingClient interface {
	Send(context.Context, *messaging.Message) (string, error)
}

type Sender struct {
	client         MessagingClient
	isUnregistered func(error) bool
	metrics        *servicemetrics.Metrics
}

func NewSender(client MessagingClient, metrics *servicemetrics.Metrics) *Sender {
	return &Sender{client: client, isUnregistered: messaging.IsUnregistered, metrics: metrics}
}

func newSender(client MessagingClient, isUnregistered func(error) bool, metrics *servicemetrics.Metrics) *Sender {
	return &Sender{client: client, isUnregistered: isUnregistered, metrics: metrics}
}

// NewADCClient initializes the Firebase Messaging client using Application Default Credentials.
// Messaging() eagerly creates the authenticated HTTP client, so missing credentials fail startup.
func NewADCClient(ctx context.Context, projectID string) (*messaging.Client, error) {
	firebaseApp, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: projectID})
	if err != nil {
		return nil, fmt.Errorf("initialize Firebase app: %w", err)
	}
	client, err := firebaseApp.Messaging(ctx)
	if err != nil {
		return nil, fmt.Errorf("initialize Firebase Messaging with ADC: %w", err)
	}
	return client, nil
}

func (s *Sender) Send(ctx context.Context, push app.Push) error {
	message, err := BuildMessage(push)
	if err != nil {
		return err
	}
	if _, err := s.client.Send(ctx, message); err != nil {
		if s.isUnregistered(err) {
			s.metrics.RecordProviderCall(servicemetrics.ProviderUnregistered)
			return app.ErrTokenUnregistered
		}
		s.metrics.RecordProviderCall(servicemetrics.ProviderError)
		return fmt.Errorf("Firebase Cloud Messaging send failed: %w", err)
	}
	s.metrics.RecordProviderCall(servicemetrics.ProviderAccepted)
	return nil
}

func BuildMessage(push app.Push) (*messaging.Message, error) {
	if push.Token == "" || push.ChatID == "" {
		return nil, errors.New("FCM push requires a token and chat ID")
	}
	message := &messaging.Message{
		Token: push.Token,
		Data:  map[string]string{"type": "sync", "chatId": push.ChatID},
	}
	switch push.Platform {
	case "ANDROID":
		message.Android = &messaging.AndroidConfig{Priority: "normal"}
	case "IOS":
		message.APNS = &messaging.APNSConfig{
			Headers: map[string]string{"apns-push-type": "background", "apns-priority": "5"},
			Payload: &messaging.APNSPayload{Aps: &messaging.Aps{ContentAvailable: true}},
		}
	case "WEB":
		message.Webpush = &messaging.WebpushConfig{Data: map[string]string{"type": "sync", "chatId": push.ChatID}}
	default:
		return nil, fmt.Errorf("unsupported push platform %q", push.Platform)
	}
	return message, nil
}
