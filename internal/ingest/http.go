package ingest

import (
	"log/slog"
	"net/http"

	"github.com/ledatu/csar-audit/internal/pipeline"
	"github.com/ledatu/csar-core/audit"
	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/httpx"
)

type submitter interface {
	Submit(*audit.Event) error
}

type httpIngestBody struct {
	Events []*audit.Event `json:"events"`
}

// HTTPHandler serves POST /ingest with a JSON batch body.
type HTTPHandler struct {
	buf    submitter
	logger *slog.Logger
}

// NewHTTPHandler constructs the HTTP ingest handler.
func NewHTTPHandler(buf *pipeline.Buffer, logger *slog.Logger) *HTTPHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HTTPHandler{buf: buf, logger: logger.With("component", "audit_ingest_http")}
}

// ServeHTTP validates the JSON batch, enqueues the events, and returns 202 Accepted.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body httpIngestBody
	if err := httpx.ReadJSON(r, &body); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if len(body.Events) == 0 {
		httpx.WriteError(w, csarerrors.Validation("events required"))
		return
	}

	for _, event := range body.Events {
		if err := Validate(event); err != nil {
			httpx.WriteError(w, csarerrors.Validation("%v", err))
			return
		}
	}

	for idx, event := range body.Events {
		if err := h.buf.Submit(event); err != nil {
			h.logger.Warn("audit ingest buffer full", "action", event.Action, "accepted", idx, "total", len(body.Events))
			httpx.WriteError(w, csarerrors.Unavailable("audit ingest buffer full after %d of %d events", idx, len(body.Events)))
			return
		}
	}

	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":   "accepted",
		"accepted": len(body.Events),
	})
}
