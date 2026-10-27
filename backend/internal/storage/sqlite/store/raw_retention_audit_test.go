package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

const rawAuditAt = "2026-10-27T10:00:00Z"
const rawAuditCutoff = "2026-10-25T11:00:00Z"

func rawAuditFixture(t *testing.T, id int64) (maintenanceRow, maintenanceAudit) {
	t.Helper()
	original := `{"method":"message.completed","providerTurnId":"native-old","providerConversationId":"same-native","turnState":"completed","error":null,"text":"UNIQUE-EXPIRED-SENTINEL","detail":{"secret":"old"},"usage":{"input":4},"unknown":{"value":null}}`
	row := maintenanceRow{"id": id, "conversation_id": "conv", "payload_json": original}
	updates, fields, err := maintenanceTransform(context.Background(), row, "raw", rawAuditAt, rawAuditCutoff)
	if err != nil {
		t.Fatal(err)
	}
	sha, err := rawFieldMapSHA(original)
	if err != nil {
		t.Fatal(err)
	}
	fieldJSON, _ := maintenanceCanonical(fields)
	side := maintenanceAudit{kind: "provider_event", id: maintenanceID(id), conversation: "conv", originalSHA: sha, at: rawAuditAt, cutoff: rawAuditCutoff, fields: fieldJSON, bytes: int64(len(original))}
	row["payload_json"] = updates["payload_json"]
	value, _ := maintenanceJSON(row.text("payload_json"))
	obj := value.(map[string]any)
	delete(obj["_retention"].(map[string]any), "field_map_audit")
	row["payload_json"], err = maintenanceCanonical(obj)
	if err != nil {
		t.Fatal(err)
	}
	return row, side
}
func TestRawAuditLosslessDomainsAndHeaders(t *testing.T) {
	for _, id := range []int64{1, -1, 9223372036854775807} {
		row, side := rawAuditFixture(t, id)
		before, _, _ := rawInlineAudit(context.Background(), row, false)
		migrated, ok := losslessRawAudit(context.Background(), row, side)
		if !ok {
			t.Fatal("eligible audit rejected")
		}
		row["payload_json"] = migrated
		after, audit, ok := rawInlineAudit(context.Background(), row, true)
		if !ok {
			t.Fatal("bound audit invalid")
		}
		if audit["original_sha256"] == side.originalSHA {
			t.Fatal("different SHA domains collapsed")
		}
		if audit["field_map_audit"].(map[string]any)["sha256"] != side.originalSHA {
			t.Fatal("canonical-map digest lost")
		}
		delete(before, "_retention")
		delete(after, "_retention")
		a, _ := maintenanceCanonical(before)
		b, _ := maintenanceCanonical(after)
		if a != b || strings.Contains(migrated, "UNIQUE-EXPIRED-SENTINEL") {
			t.Fatal("header changed/body revived")
		}
		if again, ok := losslessRawAudit(context.Background(), row, side); !ok || again != migrated {
			t.Fatal("not idempotent")
		}
	}
}
func TestRawAuditUnknownAndMismatchedFactsRetained(t *testing.T) {
	mutations := map[string]func(maintenanceRow, *maintenanceAudit){
		"no_inline": func(r maintenanceRow, a *maintenanceAudit) { r["payload_json"] = `{"error":null}` },
		"short_marker": func(r maintenanceRow, a *maintenanceAudit) {
			r["payload_json"] = `{"text":"","_retention":{"contentExpired":true,"protocol":1}}`
		},
		"foreign": func(r maintenanceRow, a *maintenanceAudit) {
			r["payload_json"] = strings.Replace(r.text("payload_json"), "ao-history-v1", "foreign", 1)
		},
		"wrong_owner":    func(r maintenanceRow, a *maintenanceAudit) { a.conversation = "other" },
		"wrong_id_alias": func(r maintenanceRow, a *maintenanceAudit) { a.id = "01" },
		"semantic":       func(r maintenanceRow, a *maintenanceAudit) { a.semantic = strings.Repeat("a", 64) },
		"size":           func(r maintenanceRow, a *maintenanceAudit) { a.bytes++ },
		"cutoff":         func(r maintenanceRow, a *maintenanceAudit) { a.cutoff = rawAuditAt },
		"digest_domain":  func(r maintenanceRow, a *maintenanceAudit) { a.originalSHA = "bad" },
		"fields":         func(r maintenanceRow, a *maintenanceAudit) { a.fields = `["text"]` },
		"unknown_extension": func(r maintenanceRow, a *maintenanceAudit) {
			r["payload_json"] = strings.Replace(r.text("payload_json"), `"protocol":"ao-history-v1"`, `"unexpected":true,"protocol":"ao-history-v1"`, 1)
		},
		"body_nonempty": func(r maintenanceRow, a *maintenanceAudit) {
			r["payload_json"] = strings.Replace(r.text("payload_json"), `"text":""`, `"text":"late"`, 1)
		},
		"duplicate_key": func(r maintenanceRow, a *maintenanceAudit) { r["payload_json"] = `{"_retention":{},"_retention":{}}` },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			row, side := rawAuditFixture(t, 1)
			mutate(row, &side)
			if _, ok := losslessRawAudit(context.Background(), row, side); ok {
				t.Fatal("unproven audit folded")
			}
		})
	}
}
func TestRawMaintenanceInlineOnlyNoBodyAndCAS(t *testing.T) {
	for _, body := range []string{`{"providerTurnId":"native-old","text":"old"}`, `{"providerTurnId":"native-old","error":null,"text":""}`} {
		w, r := maintenancePrepareFixture(t)
		ctx := context.Background()
		_, err := w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, body)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Now().UTC()
		cutoff := now.Add(-48 * time.Hour)
		plans, err := prepareMaintenanceRows(ctx, r, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, cutoff)
		if err != nil || len(plans) != 1 || plans[0].skip != "" {
			t.Fatal(plans, err)
		}
		if len(plans[0].audits) != 0 {
			t.Fatal("raw duplicate marker planned")
		}
		tx, err := w.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		result := HistoryMaintenanceResult{Retained: map[string]int{}}
		advanced, err := applyMaintenancePrepared(ctx, tx, maintenanceStages["raw"], "raw", "id", plans[0], cutoff, now, &result)
		if err != nil || !advanced {
			t.Fatal(result, err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		var markers int
		if err = w.QueryRow(`SELECT COUNT(*) FROM ao_history_retention`).Scan(&markers); err != nil || markers != 0 {
			t.Fatal(markers, err)
		}
		var after string
		w.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE id=1`).Scan(&after)
		if strings.Contains(body, `"text":"old"`) {
			row := maintenanceRow{"id": int64(1), "conversation_id": "conv", "payload_json": after}
			if _, _, ok := rawInlineAudit(ctx, row, true); !ok {
				t.Fatal("new inline is not complete")
			}
			plans, err = prepareMaintenanceRows(ctx, r, maintenanceStages["raw"], "raw", "id", []any{int64(1)}, DefaultHistoryMaintenanceOptions(), now, cutoff)
			if err != nil || !plans[0].inlineExpired {
				t.Fatal("inline-only not recognized", err)
			}
			w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, strings.Replace(after, `"text":""`, `"text":"changed"`, 1))
			tx, _ = w.BeginTx(ctx, nil)
			result = HistoryMaintenanceResult{Retained: map[string]int{}}
			advanced, err = applyMaintenancePrepared(ctx, tx, maintenanceStages["raw"], "raw", "id", plans[0], cutoff, now, &result)
			tx.Rollback()
			if err != nil || advanced {
				t.Fatal("stale inline proof advanced", err)
			}
		} else if after != body || result.Audited != 1 {
			t.Fatal("no-body event mutated/audit pretended")
		}
	}
}
func TestRawAuditCopyBatchPreviewApplyAndOtherKinds(t *testing.T) {
	w, _ := maintenancePrepareFixture(t)
	row, side := rawAuditFixture(t, 1)
	ctx := context.Background()
	if _, err := w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, row.text("payload_json")); err != nil {
		t.Fatal(err)
	}
	tx, _ := w.Begin()
	if _, err := insertMaintenanceAudits(ctx, tx, []maintenanceAudit{side, {kind: "message", id: "message", conversation: "conv", fields: `["text"]`}, {kind: "provider_event", id: "2", conversation: "conv", fields: `[]`}}); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	o := RawAuditCopyOptions{BatchRows: 10, MaxRowBytes: 1 << 20, BatchBytes: 2 << 20, Budget: time.Second}
	before := row.text("payload_json")
	preview, err := FoldRawAuditCopyBatch(ctx, w, "", o)
	if err != nil || preview.Eligible != 1 || preview.Migrated != 0 || preview.Retained != 1 {
		t.Fatal(preview, err)
	}
	var text string
	w.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE id=1`).Scan(&text)
	if text != before {
		t.Fatal("preview wrote")
	}
	o.Apply = true
	applied, err := FoldRawAuditCopyBatch(ctx, w, "", o)
	if err != nil || applied.Migrated != 1 {
		t.Fatal(applied, err)
	}
	var n int
	w.QueryRow(`SELECT COUNT(*) FROM ao_history_retention WHERE object_kind='message'`).Scan(&n)
	if n != 1 {
		t.Fatal("other marker removed")
	}
	w.QueryRow(`SELECT COUNT(*) FROM ao_history_retention WHERE object_kind='provider_event'`).Scan(&n)
	if n != 1 {
		t.Fatal("orphan/no-inline removed")
	}
	again, err := FoldRawAuditCopyBatch(ctx, w, "", o)
	if err != nil || again.Migrated != 0 {
		t.Fatal(again, err)
	}
	// Existing legacy readers parse native headers and preserve error nullness,
	// regardless of the additional optional retention-audit map.
	w.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE id=1`).Scan(&text)
	var legacy map[string]json.RawMessage
	if json.Unmarshal([]byte(text), &legacy) != nil || string(legacy["error"]) != "null" || string(legacy["providerTurnId"]) != `"native-old"` {
		t.Fatal("legacy header read changed")
	}
}
func TestRawArchiveExactIdentityNeverReplacesPayload(t *testing.T) {
	w, _ := maintenancePrepareFixture(t)
	ctx := context.Background()
	if _, err := w.Exec(`CREATE UNIQUE INDEX raw_dedupe ON conversation_provider_events(conversation_id,provider_event_id) WHERE provider_event_id<>''`); err != nil {
		t.Fatal(err)
	}
	q := gen.New(w)
	p := gen.InsertConversationProviderEventParams{ConversationID: "conv", SessionID: "owner", ProviderEventID: "same-event", Method: "message.completed", PayloadJson: `{"text":"first"}`, ReceivedAt: time.Now()}
	n, err := q.InsertConversationProviderEvent(ctx, p)
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	p.PayloadJson = `{"text":"LATE-SENTINEL"}`
	n, err = q.InsertConversationProviderEvent(ctx, p)
	if err != nil || n != 0 {
		t.Fatal(n, err)
	}
	var payload string
	w.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE provider_event_id='same-event'`).Scan(&payload)
	if payload != `{"text":"first"}` {
		t.Fatal("late same-ID archive overwrote")
	}
	p.ProviderEventID = ""
	for i := 0; i < 2; i++ {
		n, err = q.InsertConversationProviderEvent(ctx, p)
		if err != nil || n != 1 {
			t.Fatal("empty ID semantics changed", n, err)
		}
	}
}

func TestRawAuditCopyLogicalCostIsReportedSeparately(t *testing.T) {
	// 128 small planted audits exercise real table/index page allocation; this is
	// synthetic data, not an estimate of live or historical production savings.
	w, _ := maintenancePrepareFixture(t)
	ctx := context.Background()
	for i := int64(1); i <= 128; i++ {
		row, a := rawAuditFixture(t, i)
		if i == 1 {
			w.Exec(`UPDATE conversation_provider_events SET payload_json=? WHERE id=1`, row.text("payload_json"))
		} else {
			w.Exec(`INSERT INTO conversation_provider_events(id,conversation_id,session_id,payload_json,received_at)VALUES(?,'conv','owner',?,'2020-01-01T00:00:00Z')`, i, row.text("payload_json"))
		}
		tx, _ := w.Begin()
		_, err := insertMaintenanceAudits(ctx, tx, []maintenanceAudit{a})
		if err != nil {
			t.Fatal(err)
		}
		tx.Commit()
	}
	r, err := FoldRawAuditCopyBatch(ctx, w, "", RawAuditCopyOptions{128, 1 << 20, 2 << 20, 5 * time.Second, true})
	if err != nil || r.Migrated != 128 || r.LogicalBytesDelta >= 0 {
		t.Fatal("no logical audit saving", r, err)
	}
	var pageSize, pages, free int64
	for name, target := range map[string]*int64{"page_size": &pageSize, "page_count": &pages, "freelist_count": &free} {
		if err = w.QueryRow("PRAGMA " + name).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("synthetic raw audits=%d logical_delta=%d database_bytes=%d reusable_page_bytes=%d OS_reclaimed=0", r.Migrated, r.LogicalBytesDelta, pages*pageSize, free*pageSize)
}
