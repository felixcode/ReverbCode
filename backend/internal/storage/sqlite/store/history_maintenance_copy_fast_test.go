package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCopyNoBodyFastPathAvoidsOwnerReadButPreservesExactCAS(t *testing.T) {
	for _, change := range []string{"none", "same_size_body", "owner"} {
		t.Run(change, func(t *testing.T) {
			w, r := maintenancePrepareFixture(t)
			ctx := context.Background()
			body := `{"providerTurnId":"native-old","text":"","error":null}`
			if _, err := w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, body); err != nil {
				t.Fatal(err)
			}
			queries := []string{}
			now := time.Now().UTC()
			cutoff := now.Add(-48 * time.Hour)
			plans, err := prepareMaintenanceRows(ctx, maintenanceObservedDB{r, &queries}, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, cutoff, true)
			if err != nil || len(plans) != 1 || !plans[0].noRawBody || len(plans[0].audits) != 0 {
				t.Fatal(plans, err)
			}
			for _, q := range queries {
				if strings.Contains(q, "FROM conversations") || strings.Contains(q, "FROM conversation_turns") || strings.Contains(q, "FROM sessions") {
					t.Fatal("unnecessary eligibility query", q)
				}
			}
			switch change {
			case "same_size_body":
				w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, strings.Replace(body, `"text":""`, `"text":[]`, 1))
			case "owner":
				w.Exec(`UPDATE conversation_provider_events SET session_id='other' WHERE id=1`)
			}
			tx, _ := w.BeginTx(ctx, nil)
			result := HistoryMaintenanceResult{Retained: map[string]int{}}
			advanced, err := applyMaintenancePrepared(ctx, tx, maintenanceStages["raw"], "raw", "id", plans[0], cutoff, now, &result)
			tx.Rollback()
			if err != nil || result.Compacted != 0 || result.Audited != 0 || result.Markers != 0 {
				t.Fatal(result, err)
			}
			if advanced != (change == "none") {
				t.Fatal(change, advanced, result)
			}
			if change == "none" && result.Retained["raw_no_body"] != 1 {
				t.Fatal(result)
			}
		})
	}
}

func TestCopyNoBodyNeverTreatsBodyOrForeignMarkerAsEmpty(t *testing.T) {
	for _, body := range []string{`{"text":"body"}`, `{"text":"","_retention":{"protocol":"foreign"}}`, `{"text":true}`, `{"text":"","text":"body"}`} {
		w, r := maintenancePrepareFixture(t)
		w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, body)
		now := time.Now().UTC()
		plans, err := prepareMaintenanceRows(context.Background(), r, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, now.Add(-48*time.Hour), true)
		if err != nil {
			t.Fatal(err)
		}
		if len(plans) != 1 || plans[0].noRawBody {
			t.Fatal("unproven empty body", plans)
		}
	}
}

func TestOnlineNoBodyPreparationKeepsExistingPath(t *testing.T) {
	w, r := maintenancePrepareFixture(t)
	w.Exec(`UPDATE conversation_provider_events SET payload_json='{"providerTurnId":"native-old","text":""}' WHERE id=1`)
	now := time.Now().UTC()
	plans, err := prepareMaintenanceRows(context.Background(), r, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, now.Add(-48*time.Hour))
	if err != nil || len(plans) != 1 || plans[0].noRawBody {
		t.Fatal(plans, err)
	}
}

func TestCopyReadDeadlineExposesPrewritePhase(t *testing.T) {
	w, r := maintenancePrepareFixture(t)
	s := NewStore(w, r)
	o := DefaultHistoryMaintenanceOptions()
	o.ReadBudget = time.Nanosecond
	now := time.Now().UTC()
	result, err := s.MaintainHistorySnapshotBatch(context.Background(), HistoryMaintenanceRequest{Stage: "raw", Cutoff: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)}, o, now)
	if !errors.Is(err, context.DeadlineExceeded) || result.ErrorType != "deadline_exceeded" || result.Phase != "read_capability" || result.CommitState != "not_started" || result.Committed || result.ReadMS <= 0 {
		t.Fatal(result, err)
	}
}
