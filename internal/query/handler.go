// Package query exposes HTTP handlers for listing persisted audit events.
package query

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ledatu/csar-audit/internal/store"
	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/gatewayctx"
	"github.com/ledatu/csar-core/httpx"
)

// Lister is the read interface satisfied by store.Postgres.
type Lister interface {
	List(ctx context.Context, filter *store.ListFilter) (*store.ListResult, error)
	ListGroups(ctx context.Context, filter *store.ListFilter) (*store.GroupResult, error)
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
	mux.Handle(http.MethodGet+" "+h.prefix+"/groups", http.HandlerFunc(h.handleGroups))
	mux.Handle(http.MethodGet+" "+h.prefix+"/groups/", http.HandlerFunc(h.handleGroups))
	mux.Handle(http.MethodGet+" "+h.prefix, http.HandlerFunc(h.handleList))
	mux.Handle(http.MethodGet+" "+h.prefix+"/", http.HandlerFunc(h.handleList))
}

func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	filter, ok := h.parseFilter(w, r)
	if !ok {
		return
	}

	ctx, cancel := investigationContext(r.Context(), filter)
	defer cancel()
	result, err := h.store.List(ctx, filter)
	if err != nil {
		httpx.WriteError(w, csarerrors.Internalf("failed to list audit events"))
		return
	}

	result.FilterContract = "caller-v1"
	result.Window = filter.InvestigationBounds()
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (h *Handler) handleGroups(w http.ResponseWriter, r *http.Request) {
	filter, ok := h.parseFilter(w, r)
	if !ok {
		return
	}

	ctx, cancel := investigationContext(r.Context(), filter)
	defer cancel()
	result, err := h.store.ListGroups(ctx, filter)
	if err != nil {
		httpx.WriteError(w, csarerrors.Internalf("failed to group audit events"))
		return
	}

	result.FilterContract = "caller-v1"
	result.Window = filter.InvestigationBounds()
	httpx.WriteJSON(w, http.StatusOK, result)
}

func (h *Handler) parseFilter(w http.ResponseWriter, r *http.Request) (*store.ListFilter, bool) {
	id, _ := gatewayctx.FromContext(r.Context())
	if id.Subject == "" {
		httpx.WriteError(w, csarerrors.Unauthorized("not authenticated"))
		return nil, false
	}
	if h.store == nil {
		httpx.WriteError(w, csarerrors.Unavailable("audit store not configured"))
		return nil, false
	}

	q := r.URL.Query()
	filter := store.ListFilter{
		ScopeType:         q.Get("scope_type"),
		ScopeID:           q.Get("scope_id"),
		Service:           q.Get("service"),
		Actor:             q.Get("actor"),
		Action:            q.Get("action"),
		TargetType:        q.Get("target_type"),
		TargetID:          q.Get("target_id"),
		RequestID:         q.Get("request_id"),
		Category:          q.Get("category"),
		Cursor:            q.Get("cursor"),
		ExcludeCategories: getListParam(q, "exclude_category", "exclude_category[]", "exclude_categories"),
		ExcludeServices:   getListParam(q, "exclude_service", "exclude_service[]", "exclude_services"),
		ExcludeActions:    getListParam(q, "exclude_action", "exclude_action[]", "exclude_actions"),
	}

	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("status_min"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.StatusMin = n
		}
	}
	if v := q.Get("status_max"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.StatusMax = n
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

	if err := parseInvestigation(q, &filter); err != nil {
		httpx.WriteError(w, csarerrors.Validation("%s", err))
		return nil, false
	}
	return &filter, true
}

func getListParam(q map[string][]string, keys ...string) []string {
	var values []string
	for _, key := range keys {
		values = append(values, q[key]...)
	}
	return values
}

func investigationContext(ctx context.Context, f *store.ListFilter) (context.Context, context.CancelFunc) {
	if f.InvestigationActive() {
		return context.WithTimeout(ctx, 10*time.Second)
	}
	return context.WithCancel(ctx)
}
func parseIdentity(q map[string][]string, prefix string) store.IdentityFilter {
	get := func(key string) string { return strings.TrimSpace(firstValue(q[prefix+"_"+key])) }
	return store.IdentityFilter{Version: get("version"), Kind: get("kind"), ID: get("id"), OwnerSubject: get("owner_subject"), EffectiveSubject: get("effective_subject"), CredentialKind: get("credential_kind"), CredentialID: get("credential_id"), ClientID: get("client_id"), ConnectionID: get("connection_id"), Assurance: get("assurance")}
}
func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
func parseInvestigation(q map[string][]string, f *store.ListFilter) error {
	f.Initiator = parseIdentity(q, "initiator")
	f.Caller = parseIdentity(q, "caller")
	f.OriginCaller = parseIdentity(q, "origin_caller")
	references := map[string]*string{"event_id": &f.EventID, "operation_id": &f.OperationID, "parent_operation_id": &f.ParentOperationID, "trace_id": &f.TraceID, "release_event_id": &f.ReleaseEventID, "cache_status": &f.CacheStatus, "audit_phase": &f.AuditPhase}
	for key, target := range references {
		*target = strings.TrimSpace(firstValue(q[key]))
	}
	// Unknown or repeated investigation fields must not silently broaden a query.
	for key, values := range q {
		identityParam := false
		for _, prefix := range []string{"initiator_", "caller_", "origin_caller_"} {
			if strings.HasPrefix(key, prefix) {
				identityParam = true
				switch strings.TrimPrefix(key, prefix) {
				case "version", "kind", "id", "owner_subject", "effective_subject", "credential_kind", "credential_id", "client_id", "connection_id", "assurance":
				default:
					return fmt.Errorf("%s: unsupported identity filter", key)
				}
			}
		}
		_, correlationParam := references[key]
		if len(values) > 1 && (identityParam || correlationParam || f.InvestigationActive() && (key == "since" || key == "until")) {
			return fmt.Errorf("%s: expected one value", key)
		}
	}
	if f.InvestigationActive() {
		for _, key := range []string{"since", "until"} {
			if value := firstValue(q[key]); value != "" {
				if _, err := time.Parse(time.RFC3339, value); err != nil {
					return fmt.Errorf("%s: invalid RFC3339 time", key)
				}
			}
		}
	}

	return f.ValidateInvestigation(time.Now())
}
