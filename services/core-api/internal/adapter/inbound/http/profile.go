package httpadapter

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) profile(c echo.Context) error {
	user, err := s.service.Profile(c.Request().Context(), claimsFrom(c.Request()).UserID)
	if err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, map[string]string{"userId": user.ID, "phone": user.Phone, "displayName": user.DisplayName})
}

func (s *Server) setName(c echo.Context) error {
	var body struct {
		DisplayName string `json:"displayName" validate:"required,min=1,max=80"`
	}
	if err := decode(c, &body); err != nil {
		return writeInvalidRequest(c, err)
	}
	if err := s.service.SetDisplayName(c.Request().Context(), claimsFrom(c.Request()).UserID, body.DisplayName); err != nil {
		return writeServiceError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}
