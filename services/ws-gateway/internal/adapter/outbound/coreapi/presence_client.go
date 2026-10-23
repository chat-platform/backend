package coreapi

import (
	"context"
	"fmt"
	"time"

	sessionv1 "github.com/chat-platform/backend/shared/gen/session/v1"
	"google.golang.org/grpc/metadata"
)

type PresenceClient struct {
	client sessionv1.SessionServiceClient
}

func NewPresenceClient(client sessionv1.SessionServiceClient) *PresenceClient {
	return &PresenceClient{client: client}
}

func (c *PresenceClient) AuthorizePresence(ctx context.Context, token, targetUserID string) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	_, err := c.client.CheckPresenceAccess(ctx, &sessionv1.CheckPresenceAccessRequest{TargetUserId: targetUserID})
	if err != nil {
		return fmt.Errorf("Core API presence authorization RPC: %w", err)
	}
	return nil
}

func (c *PresenceClient) GetLastSeen(ctx context.Context, token, targetUserID string) (*time.Time, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	response, err := c.client.GetLastSeen(ctx, &sessionv1.GetLastSeenRequest{TargetUserId: targetUserID})
	if err != nil {
		return nil, fmt.Errorf("Core API last-seen RPC: %w", err)
	}
	if !response.GetHasLastSeen() {
		return nil, nil
	}
	value := time.UnixMilli(response.GetLastSeenUnixMilli()).UTC()
	return &value, nil
}

func (c *PresenceClient) UpdateLastSeen(ctx context.Context, userID string, value time.Time) error {
	_, err := c.client.UpdateLastSeen(ctx, &sessionv1.UpdateLastSeenRequest{
		UserId: userID, LastSeenUnixMilli: value.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("Core API update last-seen RPC: %w", err)
	}
	return nil
}
