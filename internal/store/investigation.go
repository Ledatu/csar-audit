package store

import (
	"fmt"
	"github.com/ledatu/csar-core/caller"
	"strconv"
	"strings"
	"time"
)

// IdentityFilter describes exact persisted fields, never an inferred actor type.
// Initiator uses origin when recorded and otherwise the immediate caller.
type IdentityFilter struct {
	Version          string
	Kind             string
	ID               string
	OwnerSubject     string
	EffectiveSubject string
	CredentialKind   string
	CredentialID     string
	ClientID         string
	ConnectionID     string
	Assurance        string
}

type identityField struct{ Name, JSON, Value string }

func (f *IdentityFilter) fields() []identityField {
	return []identityField{
		{"version", "version", f.Version}, {"kind", "caller_kind", f.Kind}, {"id", "caller_id", f.ID},
		{"owner_subject", "owner_subject", f.OwnerSubject}, {"effective_subject", "effective_subject", f.EffectiveSubject},
		{"credential_kind", "credential_kind", f.CredentialKind}, {"credential_id", "credential_id", f.CredentialID},
		{"client_id", "client_id", f.ClientID}, {"connection_id", "connection_id", f.ConnectionID}, {"assurance", "assurance", f.Assurance},
	}
}
func (f *IdentityFilter) active() bool {
	for _, field := range f.fields() {
		if field.Value != "" {
			return true
		}
	}
	return false
}
func (f *IdentityFilter) validate(prefix string) error {
	for _, field := range f.fields() {
		valid := caller.ValidReference(field.Value)
		if field.Name == "client_id" {
			valid = caller.ValidClientID(field.Value)
		}
		if !valid {
			return fmt.Errorf("%s_%s: invalid reference", prefix, field.Name)
		}
	}
	switch f.Kind {
	case "", "absent", string(caller.Human), string(caller.Agent), string(caller.Application), string(caller.Service), string(caller.Unknown):
	default:
		return fmt.Errorf("%s_kind: unsupported caller kind", prefix)
	}
	switch f.CredentialKind {
	case "", string(caller.Session), string(caller.APIKey), string(caller.OAuthAccess), string(caller.DelegationProof), string(caller.STS):
	default:
		return fmt.Errorf("%s_credential_kind: unsupported credential kind", prefix)
	}
	switch f.Assurance {
	case "", string(caller.Verified), string(caller.Legacy):
	default:
		return fmt.Errorf("%s_assurance: unsupported assurance", prefix)
	}
	if f.Version != "" {
		v, err := strconv.Atoi(f.Version)
		if err != nil || v < 1 || v > 9999 {
			return fmt.Errorf("%s_version: expected a version from 1 to 9999", prefix)
		}
		f.Version = strconv.Itoa(v)
	}
	return nil
}

// InvestigationWindow makes server-default bounds explicit to API consumers.
type InvestigationWindow struct {
	Since time.Time `json:"since"`
	Until time.Time `json:"until"`
}

func (f *ListFilter) InvestigationActive() bool {
	return f != nil && (f.Initiator.active() || f.Caller.active() || f.OriginCaller.active() || f.EventID != "" || f.OperationID != "" || f.ParentOperationID != "" || f.TraceID != "" || f.ReleaseEventID != "" || f.CacheStatus != "" || f.AuditPhase != "")
}
func (f *ListFilter) ValidateInvestigation(now time.Time) error {
	for _, item := range []struct {
		prefix string
		value  *IdentityFilter
	}{{"initiator", &f.Initiator}, {"caller", &f.Caller}, {"origin_caller", &f.OriginCaller}} {
		if err := item.value.validate(item.prefix); err != nil {
			return err
		}
	}
	for _, item := range []struct{ name, value string }{{"event_id", f.EventID}, {"operation_id", f.OperationID}, {"parent_operation_id", f.ParentOperationID}, {"trace_id", f.TraceID}, {"release_event_id", f.ReleaseEventID}, {"cache_status", f.CacheStatus}, {"audit_phase", f.AuditPhase}} {
		if !caller.ValidReference(item.value) {
			return fmt.Errorf("%s: invalid reference", item.name)
		}
	}
	if !f.InvestigationActive() {
		return nil
	}
	if f.Until == nil {
		until := now.UTC()
		f.Until = &until
	}
	if f.Since == nil {
		since := f.Until.Add(-24 * time.Hour)
		f.Since = &since
	}
	if f.Since.After(*f.Until) || f.Until.Sub(*f.Since) > 31*24*time.Hour {
		return fmt.Errorf("identity investigation period must be ordered and at most 31 days")
	}
	return nil
}
func (f *ListFilter) InvestigationBounds() *InvestigationWindow {
	if !f.InvestigationActive() || f.Since == nil || f.Until == nil {
		return nil
	}
	return &InvestigationWindow{Since: *f.Since, Until: *f.Until}
}

func (b *whereBuilder) addIdentity(expression string, filter *IdentityFilter) {
	for _, field := range filter.fields() {
		if field.Value == "" {
			continue
		}
		// SQL expressions and JSON keys are static service-owned constants.
		column := "(" + expression + ")->>'" + field.JSON + "'"
		if field.Name == "kind" && field.Value == "absent" {
			b.clauses = append(b.clauses, "("+column+") IS NULL")
			continue
		}
		b.addEqual(column, field.Value)
	}
}
func (b *whereBuilder) addInvestigation(f *ListFilter) {
	b.addIdentity("COALESCE(NULLIF(origin_caller,'null'::jsonb),NULLIF(caller,'null'::jsonb))", &f.Initiator)
	b.addIdentity("caller", &f.Caller)
	b.addIdentity("origin_caller", &f.OriginCaller)
	for _, field := range []struct{ column, value string }{{"id::text", f.EventID}, {"operation_id", f.OperationID}, {"parent_operation_id", f.ParentOperationID}, {"trace_id", f.TraceID}, {"metadata->>'cache_status'", f.CacheStatus}, {"metadata->>'phase'", f.AuditPhase}} {
		b.addEqual(field.column, field.value)
	}
	if strings.TrimSpace(f.ReleaseEventID) != "" {
		arg := b.nextArg(f.ReleaseEventID)
		b.clauses = append(b.clauses, "(id::text = "+arg+" OR metadata->>'release_event_id' = "+arg+")")
	}
}
