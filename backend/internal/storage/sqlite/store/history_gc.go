package store

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
)

// HistoryGCOptions deletes consumed anonymous streaming archives, never their
// message/turn projections or native/control event identities. Coverage bounds
// are optional for routine retention and mandatory in the offline CLI.
type HistoryGCOptions struct {
	Cutoff time.Time
	CoverageBefore time.Time
	MaxEventID int64
	BatchRows int
	Apply bool
}

type HistoryGCResult struct {
	Scanned int `json:"scanned"`
	Eligible int `json:"eligible"`
	Deleted int64 `json:"deleted"`
	AuditDeleted int64 `json:"audit_deleted"`
	Retained map[string]int `json:"retained"`
	NextCursor int64 `json:"next_cursor"`
	Complete bool `json:"complete"`
	Committed bool `json:"transaction_committed"`
}

const historyGCStreams = `('message.delta','reasoning.delta','command.output.delta','activity.text','activity.started','activity.completed','usage','account.rateLimits')`

type historyGCRow struct { id int64; conversation, received string }
type historyGCProtection struct { at time.Time; unknown bool }

// Only unresolved turns/interactions protect their overlapping tail. A live
// owner does not retain all of its already-consumed historical stream events.
func historyGCProtected(ctx context.Context, db maintenanceDB, conversation string) (historyGCProtection, error) {
	var p historyGCProtection
	rows, err := db.QueryContext(ctx, `SELECT CAST(requested_at AS TEXT),CAST(started_at AS TEXT)
 FROM conversation_turns WHERE conversation_id=? AND state IN ('queued','running')
 UNION ALL SELECT CAST(t.requested_at AS TEXT),CAST(t.started_at AS TEXT)
 FROM conversation_activities a JOIN conversation_turns t ON t.id=a.turn_id
 WHERE a.conversation_id=? AND a.kind IN ('approval','user_input') AND a.status='pending'
 UNION ALL SELECT CAST(created_at AS TEXT),NULL FROM conversation_activities
 WHERE conversation_id=? AND kind IN ('approval','user_input') AND status='pending' AND turn_id IS NULL
 UNION ALL SELECT CAST(created_at AS TEXT),NULL FROM conversation_edit_deliveries WHERE conversation_id=? AND state='reserved'
 UNION ALL SELECT CAST(created_at AS TEXT),NULL FROM conversation_steer_deliveries WHERE conversation_id=? AND state='reserved'`, conversation,conversation,conversation,conversation,conversation)
	if err != nil { return p,err }; defer rows.Close()
	for rows.Next() {
		var a,b sql.NullString
		if err=rows.Scan(&a,&b); err!=nil { return p,err }
		valid:=false
		for _,v:=range []sql.NullString{a,b} {
			if !v.Valid || v.String=="" { continue }
			t,e:=maintenanceTime(v.String); if e!=nil { p.unknown=true; continue }
			valid=true; if p.at.IsZero() || t.Before(p.at) { p.at=t }
		}
		if !valid { p.unknown=true }
	}
	return p,rows.Err()
}

// PruneAnonymousHistoryBatch is an application-owned lifecycle mutation. All
// predicates, current pending bounds, archive and audit deletes share one AO
// writer transaction. No per-event audit is generated for expired debug logs.
func (s *Store) PruneAnonymousHistoryBatch(ctx context.Context, cursor int64, o HistoryGCOptions) (r HistoryGCResult, err error) {
	r.NextCursor=cursor; r.Retained=map[string]int{}
	defer func(){if err!=nil{r.NextCursor=cursor;r.Deleted=0;r.AuditDeleted=0;r.Committed=false}}()
	if s.readOnly && o.Apply { return r,errors.New("read_only_store") }
	if o.BatchRows<1 || o.BatchRows>10000 || o.Cutoff.IsZero() || o.Cutoff.After(time.Now().UTC().Add(-48*time.Hour)) || cursor<0 || o.MaxEventID<0 { return r,errors.New("invalid_gc_options") }
	if err=s.writeMu.LockContext(ctx); err!=nil { return r,err }; defer s.writeMu.Unlock()
	tx,err:=s.writeDB.BeginTx(ctx,&sql.TxOptions{ReadOnly:!o.Apply}); if err!=nil { return r,err }; defer tx.Rollback()
	maxID:=o.MaxEventID; if maxID==0 { maxID=1<<63-1 }
	rows,err:=tx.QueryContext(ctx, `SELECT id,conversation_id,CAST(received_at AS TEXT) FROM conversation_provider_events WHERE id>? AND id<=? AND provider_event_id='' AND method IN `+historyGCStreams+` ORDER BY id LIMIT ?`,cursor,maxID,o.BatchRows)
	if err!=nil { return r,err }
	var items []historyGCRow
	for rows.Next() { var v historyGCRow; if err=rows.Scan(&v.id,&v.conversation,&v.received);err!=nil { rows.Close();return r,err };items=append(items,v) }
	err=rows.Err();rows.Close();if err!=nil { return r,err }
	r.Complete=len(items)<o.BatchRows
	protection:=map[string]historyGCProtection{}; var ids []any; var textIDs []any
	for _,v:=range items {
		r.Scanned++;r.NextCursor=v.id
		reason,e:=historyGCEligible(ctx,tx,v,o,protection);if e!=nil{return r,e};if reason!="" {r.Retained[reason]++;continue}
		ids=append(ids,v.id);textIDs=append(textIDs,strconv.FormatInt(v.id,10));r.Eligible++
	}
	if !o.Apply || len(ids)==0 { return r,nil }
	placeholders:=strings.TrimSuffix(strings.Repeat("?,",len(ids)),",")
	result,err:=tx.ExecContext(ctx,`DELETE FROM ao_history_retention WHERE object_kind='provider_event' AND object_id IN (`+placeholders+`)`,textIDs...)
	if err!=nil { return r,err };auditDeleted,err:=result.RowsAffected();if err!=nil { return r,err }
	result,err=tx.ExecContext(ctx,`DELETE FROM conversation_provider_events WHERE id IN (`+placeholders+`) AND provider_event_id='' AND method IN `+historyGCStreams,ids...)
	if err!=nil { return r,err };deleted,err:=result.RowsAffected();if err!=nil { return r,err }
	if deleted!=int64(len(ids)) { return r,errors.New("gc_candidates_changed") }
	if err=tx.Commit();err!=nil { return r,err }
	r.Deleted=deleted;r.AuditDeleted=auditDeleted;r.Committed=true;return r,nil
}

// The offline and routine paths share this policy; routine transactions always
// compute current protection again after non-queued writer admission.
func historyGCEligible(ctx context.Context, db maintenanceDB, v historyGCRow, o HistoryGCOptions, protection map[string]historyGCProtection)(string,error){
 stamp,e:=maintenanceTime(v.received);if e!=nil{return "invalid_timestamp",nil}
 if stamp.After(o.Cutoff){return "within_48h",nil}
 p,ok:=protection[v.conversation];if !ok{p,e=historyGCProtected(ctx,db,v.conversation);if e!=nil{return "",e};protection[v.conversation]=p}
 if p.unknown{return "unknown_pending_boundary",nil};if !p.at.IsZero()&&!stamp.Before(p.at){return "unresolved_tail",nil}
 if !o.CoverageBefore.IsZero(){var at string;e=db.QueryRowContext(ctx,`SELECT expired_at FROM ao_history_retention WHERE object_kind='provider_event' AND object_id=?`,strconv.FormatInt(v.id,10)).Scan(&at);if e!=nil&&!errors.Is(e,sql.ErrNoRows){return "",e};if e==nil{t,x:=maintenanceTime(at);if x!=nil||t.After(o.CoverageBefore){return "audit_outside_backup",nil}}}
 return "",nil
}

func historyGCStreamMethod(method string) bool {
 switch method {case "message.delta","reasoning.delta","command.output.delta","activity.text","activity.started","activity.completed","usage","account.rateLimits":return true};return false
}
