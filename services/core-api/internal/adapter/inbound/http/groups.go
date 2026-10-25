package httpadapter

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

type createGroupRequest struct {
	Name          string   `json:"name" validate:"required,min=1,max=80"`
	MemberUserIDs []string `json:"memberUserIds" validate:"max=100,dive,required,uuid"`
}

type groupMembersRequest struct {
	UserIDs []string `json:"userIds" validate:"required,min=1,max=100,dive,required,uuid"`
}

type updateGroupRequest struct {
	Name       *string `json:"name" validate:"omitempty,min=1,max=80"`
	ProfilePic *string `json:"profilePic" validate:"omitempty,max=2048"`
}

func (s *Server) groupChats(c echo.Context) error {
	chats, err := s.service.GroupChats(c.Request().Context(), claimsFrom(c.Request()).UserID)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, chats)
}

func (s *Server) createGroup(c echo.Context) error {
	var body createGroupRequest
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	body.Name = strings.TrimSpace(body.Name)
	if body.Name == "" {
		return s.writeInvalidRequest(c, errors.New("group name must not be blank"))
	}
	chatID, err := s.service.CreateGroup(c.Request().Context(), claimsFrom(c.Request()).UserID, body.Name, body.MemberUserIDs)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusCreated, map[string]string{"chatId": chatID})
}

func (s *Server) updateGroup(c echo.Context) error {
	var body updateGroupRequest
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if body.Name != nil {
		trimmed := strings.TrimSpace(*body.Name)
		if trimmed == "" {
			return s.writeInvalidRequest(c, errors.New("group name must not be blank"))
		}
		body.Name = &trimmed
	}
	if err := s.service.UpdateGroup(c.Request().Context(), claimsFrom(c.Request()).UserID, c.Param("id"), body.Name, body.ProfilePic); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) addGroupMembers(c echo.Context) error {
	var body groupMembersRequest
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if err := s.service.AddGroupMembers(c.Request().Context(), claimsFrom(c.Request()).UserID, c.Param("id"), body.UserIDs); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) removeGroupMember(c echo.Context) error {
	if err := s.service.RemoveGroupMember(c.Request().Context(), claimsFrom(c.Request()).UserID, c.Param("id"), c.Param("userId")); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) addGroupAdmins(c echo.Context) error {
	var body groupMembersRequest
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	if err := s.service.SetGroupAdmins(c.Request().Context(), claimsFrom(c.Request()).UserID, c.Param("id"), body.UserIDs, true); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Server) removeGroupAdmin(c echo.Context) error {
	if err := s.service.SetGroupAdmins(c.Request().Context(), claimsFrom(c.Request()).UserID, c.Param("id"), []string{c.Param("userId")}, false); err != nil {
		return s.writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}
