package ingest

import (
	"log/slog"
	"net/http"

	"github.com/ledatu/csar-audit/internal/pipeline"
	"github.com/ledatu/csar-core/audit"
	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/httpx"
)

// HTTPHandler serves POST /ingest with a JSON audit.Event body.
type HTTPHandler struct {
	buf    *pipeline.Buffer
	logger *slog.Logger
}

// NewHTTPHandler constructs the HTTP ingest handler.
func NewHTTPHandler(buf *pipeline.Buffer, logger *slog.Logger) *HTTPHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HTTPHandler{buf: buf, logger: logger.With("component", "audit_ingest_http")}
}

// ServeHTTP validates the JSON body, enqueues the event, and returns 202 Accepted.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body audit.Event
	if err := httpx.ReadJSON(r, &body); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if err := Validate(&body); err != nil {
		httpx.WriteError(w, csarerrors.Validation("%v", err))
		return
	}
	if err := h.buf.Submit(&body); err != nil {
		h.logger.Warn("audit ingest buffer full", "action", body.Action)
		httpx.WriteError(w, csarerrors.Unavailable("audit ingest buffer full"))
		return
	}
	httpx.WriteJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}
