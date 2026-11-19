package app

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"
)

type GroupMember struct {
	UserID      string `json:"userId"`
	PhoneNumber string `json:"phoneNumber"`
	DisplayName string `json:"displayName"`
	Role        string `json:"role"`
}

type GroupMembershipState struct {
	ChatID     string     `json:"chatId"`
	Status     string     `json:"status"`
	Role       string     `json:"role,omitempty"`
	Revision   int64      `json:"revision"`
	Name       string     `json:"name,omitempty"`
	ProfilePic *string    `json:"profilePic"`
	UpdatedAt  *time.Time `json:"updatedAt,omitempty"`
}

type GroupChat struct {
	ID         string        `json:"chatId"`
	Name       string        `json:"name"`
	ProfilePic *string       `json:"profilePic"`
	UpdatedAt  time.Time     `json:"updatedAt"`
	Role       string        `json:"role"`
	Revision   int64         `json:"revision"`
	Members    []GroupMember `json:"members"`
}

func (s *Service) GroupMembershipState(ctx context.Context, userID string, groupIDs []string) ([]GroupMembershipState, error) {
	if !validUUID(userID) {
		return nil, ErrInvalidRequest
	}
	for _, id := range groupIDs {
		if !validUUID(id) {
			return nil, ErrInvalidRequest
		}
	}
	return s.repo.ListGroupMembershipState(ctx, userID, groupIDs)
}

func (s *Service) GroupChats(ctx context.Context, userID string) ([]GroupChat, error) {
	return s.repo.ListGroupChats(ctx, userID)
}

func (s *Service) GroupChatsByID(ctx context.Context, userID string, groupIDs []string) ([]GroupChat, error) {
	if !validUUID(userID) || len(groupIDs) == 0 || len(groupIDs) > 100 {
		return nil, ErrInvalidRequest
	}
	for _, id := range groupIDs {
		if !validUUID(id) {
			return nil, ErrInvalidRequest
		}
	}
	return s.repo.ListGroupChatsByID(ctx, userID, groupIDs)
}

func (s *Service) CreateGroup(ctx context.Context, creatorID, name string, memberIDs []string) (string, error) {
	name = strings.TrimSpace(name)
	if !validUUID(creatorID) {
		return "", ErrInvalidRequest
	}
	if err := validateUniqueUserIDs(memberIDs, creatorID); err != nil {
		return "", err
	}
	return s.repo.CreateGroup(ctx, creatorID, name, memberIDs)
}

func (s *Service) UpdateGroup(ctx context.Context, actorID, groupID string, name, profilePic *string) error {
	if !validUUID(actorID) || !validUUID(groupID) || (name == nil && profilePic == nil) {
		return ErrInvalidRequest
	}
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		name = &trimmed
	}
	if profilePic != nil {
		trimmed := strings.TrimSpace(*profilePic)
		profilePic = &trimmed
	}
	return s.repo.UpdateGroup(ctx, actorID, groupID, name, profilePic)
}

func (s *Service) AddGroupMembers(ctx context.Context, actorID, groupID string, memberIDs []string) error {
	if !validUUID(actorID) || !validUUID(groupID) || len(memberIDs) == 0 || len(memberIDs) > 100 {
		return ErrInvalidRequest
	}
	if err := validateUniqueUserIDs(memberIDs, ""); err != nil {
		return err
	}
	return s.repo.AddGroupMembers(ctx, actorID, groupID, memberIDs)
}

func (s *Service) RemoveGroupMember(ctx context.Context, actorID, groupID, memberID string) error {
	if !validUUID(actorID) || !validUUID(groupID) || !validUUID(memberID) {
		return ErrInvalidRequest
	}
	return s.repo.RemoveGroupMember(ctx, actorID, groupID, memberID)
}

func (s *Service) SetGroupAdmins(ctx context.Context, actorID, groupID string, memberIDs []string, admin bool) error {
	if !validUUID(actorID) || !validUUID(groupID) || len(memberIDs) == 0 || len(memberIDs) > 100 {
		return ErrInvalidRequest
	}
	if err := validateUniqueUserIDs(memberIDs, ""); err != nil {
		return err
	}
	return s.repo.SetGroupAdmins(ctx, actorID, groupID, memberIDs, admin)
}

func validateUniqueUserIDs(ids []string, forbiddenID string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !validUUID(id) || id == forbiddenID {
			return ErrInvalidRequest
		}
		if _, exists := seen[id]; exists {
			return ErrInvalidRequest
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validUUID(value string) bool {
	_, err := uuid.Parse(value)
	return err == nil
}
