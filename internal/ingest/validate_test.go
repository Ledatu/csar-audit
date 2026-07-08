package ingest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ledatu/csar-core/audit"
)

func TestValidateRejectsOversizedPayloads(t *testing.T) {
	ev := &audit.Event{
		Actor:      "user",
		Action:     "POST /x",
		TargetType: "path",
		ScopeType:  "platform",
		BeforeState: json.RawMessage(`"` + strings.Repeat("a", maxBeforeStateBytes+1) + `"`),
	}
	if err := Validate(ev); err == nil {
		t.Fatal("expected before_state size error")
	}

	ev.BeforeState = nil
	ev.Metadata = json.RawMessage(`"` + strings.Repeat("b", maxMetadataBytes+1) + `"`)
	if err := Validate(ev); err == nil {
		t.Fatal("expected metadata size error")
	}
}
