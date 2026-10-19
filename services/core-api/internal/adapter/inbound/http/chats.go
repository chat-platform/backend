package httpadapter

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) directChat(c echo.Context) error {
	var body struct {
		Phone string `json:"phone"`
	}
	if !decode(c, &body) {
		return nil
	}
	chatID, err := s.service.DirectChat(c.Request().Context(), claimsFrom(c.Request()).UserID, body.Phone)
	if err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]string{"chatId": chatID})
}
