// Package query exposes HTTP handlers for listing persisted audit events.
package query

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/ledatu/csar-core/audit"
	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/gatewayctx"
	"github.com/ledatu/csar-core/httpx"
)

// Lister is the read interface satisfied by store.Postgres.
type Lister interface {
	List(ctx context.Context, filter *audit.ListFilter) (*audit.ListResult, error)
}

// Handler serves GET /admin/audit. Authorization is enforced by the router (x-csar-authz);
// this handler requires a gateway subject for defense in depth.
type Handler struct {
	store  Lister
	prefix string
}

// New constructs a query Handler.
func New(st Lister) *Handler {
	return &Handler{store: st, prefix: "/admin/audit"}
}

// Register attaches routes to mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.Handle(http.MethodGet+" "+h.prefix, http.HandlerFunc(h.handleList))
	mux.Handle(http.MethodGet+" "+h.prefix+"/", http.HandlerFunc(h.handleList))
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	id, _ := gatewayctx.FromContext(r.Context())
	if id.Subject == "" {
		httpx.WriteError(w, csarerrors.Unauthorized("not authenticated"))
		return
	}
	if h.store == nil {
		httpx.WriteError(w, csarerrors.Unavailable("audit store not configured"))
		return
	}

	q := r.URL.Query()
	filter := audit.ListFilter{
		ScopeType:  q.Get("scope_type"),
		ScopeID:    q.Get("scope_id"),
		Service:    q.Get("service"),
		Actor:      q.Get("actor"),
		Action:     q.Get("action"),
		TargetType: q.Get("target_type"),
		TargetID:   q.Get("target_id"),
		RequestID:  q.Get("request_id"),
		Cursor:     q.Get("cursor"),
	}

	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.Since = &t
		}
	}
	if v := q.Get("until"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.Until = &t
		}
	}

	result, err := h.store.List(r.Context(), &filter)
	if err != nil {
		httpx.WriteError(w, csarerrors.Internalf("failed to list audit events"))
		return
	}

	httpx.WriteJSON(w, http.StatusOK, result)
}
