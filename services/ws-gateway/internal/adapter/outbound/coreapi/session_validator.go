package coreapi

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	sessionv1 "github.com/chat-platform/backend/shared/gen/session/v1"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type SessionValidator struct {
	client sessionv1.SessionServiceClient
}

func NewSessionValidator(client sessionv1.SessionServiceClient) *SessionValidator {
	return &SessionValidator{client: client}
}

func (v *SessionValidator) ValidateSession(ctx context.Context, token string) (sessiontoken.Claims, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	response, err := v.client.ValidateSession(ctx, &sessionv1.ValidateSessionRequest{})
	if err != nil {
		if code := status.Code(err); code == codes.Unauthenticated || code == codes.PermissionDenied {
			return sessiontoken.Claims{}, fmt.Errorf("%w: Core API rejected session validation: %w", domain.ErrInvalidIdentity, err)
		}
		return sessiontoken.Claims{}, fmt.Errorf("Core API ValidateSession RPC: %w", err)
	}
	if response == nil {
		return sessiontoken.Claims{}, errors.New("Core API ValidateSession RPC returned an empty response")
	}
	return sessiontoken.Claims{
		SessionID: response.GetSessionId(), UserID: response.GetUserId(), DeviceID: response.GetDeviceId(),
		IssuedAt: response.GetIssuedAt(), ExpiresAt: response.GetExpiresAt(),
	}, nil
}
