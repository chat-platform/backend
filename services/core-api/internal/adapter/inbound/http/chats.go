package httpadapter

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) directChat(c echo.Context) error {
	var body struct {
		Phone string `json:"phone" validate:"required,min=6,max=32"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	chatID, err := s.service.DirectChat(c.Request().Context(), claimsFrom(c.Request()).UserID, body.Phone)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]string{"chatId": chatID})
}

func (s *Server) directChatPeer(c echo.Context) error {
	peerID, err := s.service.DirectChatPeer(c.Request().Context(), claimsFrom(c.Request()).UserID, c.Param("id"))
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]string{"peerUserId": peerID})
}
