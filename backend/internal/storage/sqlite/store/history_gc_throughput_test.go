package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
	"path/filepath"
	"testing"
	"time"
)

func TestHistoryGCBatchCapIsIndependentAndPrefixExact(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedProject(t, s, "gc-cap")
	rec := sampleRecord("gc-cap")
	rec.Mode = domain.SessionModeChat
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "gc-cap-conv", domain.ConversationScopeSession, "gc-cap", session.ID, histClock)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 300; i++ {
		method := "turn.completed"
		if i%10 == 0 {
			method = "message.delta"
		}
		if _, err = tx.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,'',?,'PRIVATE-NON-JSON',?)`, conv.ID, session.ID, method, histClock); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	cap := s.HistoryRetentionCapability()
	o := adapter.DefaultHistoryMaintenanceOptions()
	o.BatchRows = 25
	o.GCBatchRows = 250
	req := adapter.HistoryMaintenanceRequest{Stage: "gc:raw", Cutoff: time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano), DatabaseDevice: cap["dbDevice"].(uint64), DatabaseInode: cap["dbInode"].(uint64), BatchRows: 999}
	got, err := s.MaintainHistoryBatch(ctx, req, o)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Committed || got.Scanned != 250 || got.Deleted != 25 || got.NextCursor != "250" || got.Retained["control_event"] != 225 {
		t.Fatalf("GC prefix/cap %+v", got)
	}
	t.Logf("gc_rows=%d deleted=%d read_ms=%f held_ms=%f reserved_ms=%f", got.Scanned, got.Deleted, got.ReadMS, got.HeldMS, got.WriterReservedMS)
	req.Cursor = got.NextCursor
	req.BatchRows = 17
	got, err = s.MaintainHistoryBatch(ctx, req, o)
	if err != nil || got.Scanned != 17 || got.NextCursor != "267" || got.Deleted != 1 {
		t.Fatalf("lower client cap %+v %v", got, err)
	}
	req.Stage = "raw"
	req.Cursor = "0"
	req.BatchRows = 250
	got, err = s.MaintainHistoryBatch(ctx, req, o)
	if err != nil || got.Scanned > 25 {
		t.Fatalf("GC raised body cap %+v %v", got, err)
	}
	// Every untouched eligible row after the confirmed cursor remains available.
	var remaining int
	db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE id>267 AND method='message.delta'`).Scan(&remaining)
	if remaining != 4 {
		t.Fatal(fmt.Sprint("cursor skipped remaining eligible rows ", remaining))
	}
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 250; i++ {
		if _, err = tx.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,'','message.delta','PRIVATE-NON-JSON',?)`, conv.ID, session.ID, histClock); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	req.Stage = "gc:raw"
	req.Cursor = "300"
	req.BatchRows = 250
	got, err = s.MaintainHistoryBatch(ctx, req, o)
	if err != nil || !got.Committed || got.Scanned != 250 || got.Deleted != 250 || got.NextCursor != "550" {
		t.Fatalf("GC full mutation batch %+v %v", got, err)
	}
	t.Logf("gc_full_delete_rows=%d held_ms=%f reserved_ms=%f", got.Deleted, got.HeldMS, got.WriterReservedMS)

}

func TestHistoryGCBatchCapRejectsUnsafeBounds(t *testing.T) {
	for _, n := range []int{0, -1, 1001} {
		o := adapter.DefaultHistoryMaintenanceOptions()
		o.GCBatchRows = n
		if o.Validate() == nil {
			t.Fatal("invalid GC cap", n)
		}
	}
	o := adapter.DefaultHistoryMaintenanceOptions()
	o.GCBatchRows = 50
	if o.Validate() != nil || o.BatchRows != 100 || o.WriteBudget != 250*time.Millisecond {
		t.Fatal("GC configuration changed body budget")
	}
}
