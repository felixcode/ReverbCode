package store

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSnapshotFactsOnlyRawSnapshotAndPerKey(t *testing.T) {
	w, r := maintenancePrepareFixture(t)
	for _, stage := range []string{"raw", "message", "session", "builtin:telemetry_event"} {
		for _, snapshot := range []bool{false, true} {
			db := maintenanceSnapshotEligibility(r, stage, snapshot)
			_, cached := db.(*maintenanceSnapshotFacts)
			if cached != (snapshot && stage == "raw") {
				t.Fatal("unexpected cache scope")
			}
		}
	}
	if _, err := w.Exec(`INSERT INTO review_run VALUES('other','running'); INSERT INTO conversation_turns(id,conversation_id,state) VALUES('other-turn','other-conv','running')`); err != nil {
		t.Fatal(err)
	}
	queries := []string{}
	db := maintenanceSnapshotEligibility(maintenanceObservedDB{r, &queries}, "raw", true)
	for i := 0; i < 3; i++ {
		for _, key := range []string{"owner", "other"} {
			got, err := maintenanceReviewActive(context.Background(), db, key)
			if err != nil || got != (key == "other") {
				t.Fatal("review fact mismatch", err)
			}
		}
		for _, key := range []string{"conv", "other-conv"} {
			got, err := maintenanceConversationLive(context.Background(), db, key)
			if err != nil || got != (key == "other-conv") {
				t.Fatal("turn fact mismatch", err)
			}
		}
	}
	if len(queries) != 4 {
		t.Fatal("facts not scoped by query and key", len(queries))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := maintenanceReviewActive(ctx, db, "owner"); !errors.Is(err, context.Canceled) {
		t.Fatal("cached fact hid cancellation", err)
	}
}

func TestSnapshotFactsNeverCacheErrorsAndFreshScopeRechecks(t *testing.T) {
	w, r := maintenancePrepareFixture(t)
	db := maintenanceSnapshotEligibility(r, "raw", true)
	if _, err := w.Exec(`DROP TABLE review_run`); err != nil {
		t.Fatal(err)
	}
	if _, err := maintenanceReviewActive(context.Background(), db, "owner"); err == nil {
		t.Fatal("missing error")
	}
	if _, err := w.Exec(`CREATE TABLE review_run(session_id TEXT,status TEXT); INSERT INTO review_run VALUES('owner','running')`); err != nil {
		t.Fatal(err)
	}
	if got, err := maintenanceReviewActive(context.Background(), db, "owner"); err != nil || !got {
		t.Fatal("error was cached", err)
	}
	if _, err := w.Exec(`UPDATE review_run SET status='completed'`); err != nil {
		t.Fatal(err)
	}
	fresh := maintenanceSnapshotEligibility(r, "raw", true)
	if got, err := maintenanceReviewActive(context.Background(), fresh, "owner"); err != nil || got {
		t.Fatal("facts escaped scope", err)
	}
}

func TestSnapshotFactsPreservePlansForProtectedUnknownAndEmptyRaw(t *testing.T) {
	for _, kind := range []string{"closed", "review", "running", "unknown", "no_body", "foreign_marker"} {
		t.Run(kind, func(t *testing.T) {
			w, r := maintenancePrepareFixture(t)
			if _, err := w.Exec(`UPDATE sessions SET is_terminated=1`); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "review":
				w.Exec(`INSERT INTO review_run VALUES('owner','running')`)
			case "running":
				w.Exec(`UPDATE conversation_turns SET state='running'`)
			case "unknown":
				w.Exec(`UPDATE conversation_provider_events SET session_id='missing'`)
			case "no_body":
				w.Exec(`UPDATE conversation_provider_events SET payload_json='{"text":""}'`)
			case "foreign_marker":
				w.Exec(`UPDATE conversation_provider_events SET payload_json='{"text":"body","_retention":{"protocol":"foreign"}}'`)
			}
			now := time.Now().UTC()
			cutoff := now.Add(-48 * time.Hour)
			baseline, err := prepareMaintenanceRows(context.Background(), r, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, cutoff, true)
			if err != nil {
				t.Fatal(err)
			}
			cached, err := prepareMaintenanceRows(context.Background(), maintenanceSnapshotEligibility(r, "raw", true), maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, cutoff, true)
			if err != nil || !reflect.DeepEqual(baseline, cached) {
				t.Fatal("preparation changed", err)
			}
		})
	}
}

func TestSnapshotFactsWriterRechecksLifecycleMetadataBodyAndMarker(t *testing.T) {
	for _, change := range []string{"none", "review", "live_turn", "handoff", "pending", "owner", "same_size_body", "marker"} {
		t.Run(change, func(t *testing.T) {
			w, r := maintenancePrepareFixture(t)
			if _, err := w.Exec(`UPDATE sessions SET is_terminated=1`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			now := time.Now().UTC()
			cutoff := now.Add(-48 * time.Hour)
			reader, _ := r.BeginTx(ctx, nil)
			readFacts := maintenanceSnapshotEligibility(reader, "raw", true)
			plans, err := prepareMaintenanceRows(ctx, readFacts, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, cutoff, true)
			reader.Rollback()
			if err != nil || len(plans) != 1 || plans[0].skip != "" {
				t.Fatal("prepare failed", err)
			}
			queries := map[string]string{
				"review":         `INSERT INTO review_run VALUES('owner','running')`,
				"live_turn":      `UPDATE conversation_turns SET state='running'`,
				"handoff":        `INSERT INTO agent_switches VALUES('owner','started')`,
				"pending":        `INSERT INTO conversation_activities(id,conversation_id,turn_id,kind,status) VALUES('pending','conv','turn','approval','pending')`,
				"owner":          `UPDATE conversation_provider_events SET session_id='missing'`,
				"same_size_body": `UPDATE conversation_provider_events SET payload_json=replace(payload_json,'body-old','body-new')`,
				"marker":         `INSERT INTO ao_history_retention(object_kind,object_id,conversation_id,semantic_sha256,prompt_sha256,original_bytes,original_sha256,expired_at,cutoff_at,fields_json) VALUES('provider_event','1','conv','','',0,'existing','2020','2020','[]')`,
			}
			if q := queries[change]; q != "" {
				if _, err = w.Exec(q); err != nil {
					t.Fatal("fixture change failed", err)
				}
			}
			writer, _ := w.BeginTx(ctx, nil)
			observed := []string{}
			writeFacts := maintenanceSnapshotEligibility(maintenanceObservedDB{writer, &observed}, "raw", true)
			result := HistoryMaintenanceResult{Retained: map[string]int{}}
			_, err = applyMaintenancePreparedWithEligibility(ctx, writer, writeFacts, maintenanceStages["raw"], "raw", "id", plans[0], cutoff, now, &result)
			writer.Rollback()
			if err != nil || (result.Compacted == 1) != (change == "none") || (change != "none" && result.Markers != 0) {
				t.Fatal("stale facts affected write", change, result, err)
			}
			if change == "none" || change == "review" || change == "live_turn" {
				reviewed := false
				for _, q := range observed {
					reviewed = reviewed || strings.Contains(q, "FROM review_run")
				}
				if !reviewed {
					t.Fatal("writer did not recheck review fact")
				}
			}
		})
	}
}
