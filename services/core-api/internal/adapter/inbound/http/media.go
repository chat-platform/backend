package httpadapter

import (
	"net/http"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/labstack/echo/v4"
)

func (s *Server) createMediaUpload(c echo.Context) error {
	var body struct {
		Category    app.MediaCategory `json:"category" validate:"required,oneof=image audio video document"`
		Size        int64             `json:"size" validate:"required,min=1"`
		ContentType string            `json:"contentType" validate:"required,max=255"`
	}
	if err := decode(c, &body); err != nil {
		return s.writeInvalidRequest(c, err)
	}
	result, err := s.service.CreateMediaUpload(c.Request().Context(), body.Category, body.Size, body.ContentType)
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, result)
}

func (s *Server) createMediaDownload(c echo.Context) error {
	result, err := s.service.CreateMediaDownload(c.Request().Context(), c.Param("id"))
	if err != nil {
		return s.writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, result)
}
