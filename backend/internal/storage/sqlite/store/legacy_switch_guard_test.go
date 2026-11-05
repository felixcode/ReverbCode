package store

import (
	"context"
	"database/sql"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"path/filepath"
	"testing"
)

func TestLegacySwitchRecoveryFenceReadsExactOwnerWithoutMutation(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "fixture.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE agent_switches(id TEXT,session_id TEXT,state TEXT);CREATE INDEX switch_owner ON agent_switches(session_id);
 CREATE TABLE agent_switch_failure_outbox(switch_id TEXT,gate_retained TEXT,delivered_at TEXT,discarded_at TEXT);CREATE INDEX failure_switch ON agent_switch_failure_outbox(switch_id);
 CREATE TABLE session_interface_transitions(session_id TEXT,phase TEXT);CREATE INDEX transition_owner ON session_interface_transitions(session_id);
 CREATE TABLE agent_native_sessions(id TEXT,native_session_id TEXT);INSERT INTO agent_native_sessions VALUES('same-native','preserved-native');
 CREATE TABLE pending_inputs(id TEXT,state TEXT);INSERT INTO pending_inputs VALUES('original-input','approved');
 INSERT INTO agent_switches VALUES('legacy','owner','starting_target'),('terminal','pending-failure','completed'),('foreign','other','starting_target');
 INSERT INTO agent_switch_failure_outbox VALUES('terminal','true',NULL,NULL);
 INSERT INTO session_interface_transitions VALUES('interface-owner','recovery_required'),('recovered-owner','recovery_required'),('recovered-owner','completed');`)
	if err != nil {
		t.Fatal(err)
	}
	st := NewStore(db, db)
	for _, tc := range []struct {
		id   string
		want bool
	}{{"owner", true}, {"pending-failure", true}, {"interface-owner", true}, {"recovered-owner", false}, {"clear-owner", false}} {
		got, err := st.LegacySwitchRecoveryRequired(context.Background(), domain.SessionID(tc.id))
		if err != nil || got != tc.want {
			t.Fatalf("fence: got=%v err=%v", got, err)
		}
	}
	var state, native, input string
	if err = db.QueryRow(`SELECT state FROM agent_switches WHERE id='legacy'`).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT native_session_id FROM agent_native_sessions WHERE id='same-native'`).Scan(&native); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT state FROM pending_inputs WHERE id='original-input'`).Scan(&input); err != nil {
		t.Fatal(err)
	}
	if state != "starting_target" || native != "preserved-native" || input != "approved" {
		t.Fatal("recovery evidence mutated")
	}
}
