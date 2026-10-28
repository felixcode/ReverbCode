package store

import (
 "context"
 "database/sql"
 "errors"
 "reflect"
 "time"
)

// These reads contain no event payload. Selection is a bounded PK prefix,
// including retained records, so finding the next eligible event cannot turn
// one maintenance request into an unbounded scan while holding the writer.
func historyGCMetadata(ctx context.Context,db maintenanceDB,id any)(maintenanceRow,error){
 var conversation,received,handle,method string
 err:=db.QueryRowContext(ctx,`SELECT conversation_id,CAST(received_at AS TEXT),provider_event_id,method FROM conversation_provider_events WHERE id=?`,id).Scan(&conversation,&received,&handle,&method)
 if errors.Is(err,sql.ErrNoRows){return nil,nil};if err!=nil{return nil,err}
 return maintenanceRow{"id":id,"conversation_id":conversation,"received_at":received,"provider_event_id":handle,"method":method},nil
}
func prepareHistoryGC(ctx context.Context,db maintenanceDB,ids []any)([]maintenancePrepared,error){
 plans:=make([]maintenancePrepared,0,len(ids))
 for _,id:=range ids{row,e:=historyGCMetadata(ctx,db,id);if e!=nil{return nil,e};plans=append(plans,maintenancePrepared{id:id,row:row})}
 return plans,nil
}
func applyHistoryGC(ctx context.Context,tx *sql.Tx,p maintenancePrepared,cutoff time.Time,result *HistoryMaintenanceResult)(bool,error){
 row,e:=historyGCMetadata(ctx,tx,p.id);if e!=nil{return false,e};if row==nil{return true,nil}
 if !reflect.DeepEqual(row,p.row){result.Retained["concurrent_body_or_owner_change"]++;return false,nil}
 if row.text("provider_event_id")!=""{result.Retained["native_event_identity"]++;return true,nil}
 if !historyGCStreamMethod(row.text("method")){result.Retained["control_event"]++;return true,nil}
 if reason:=historyBodyCoverageReason(ctx,row,"gc:raw");reason!=""{result.Retained[reason]++;return true,nil}
 reason,e:=historyGCEligible(ctx,tx,historyGCRow{conversation:row.text("conversation_id"),received:row.text("received_at")},HistoryGCOptions{Cutoff:cutoff},result.rawCacheWriterProtection())
 if e!=nil{return false,e};if reason!=""{result.Retained[reason]++;return true,nil}
 // Both deletes are part of the existing maintenance transaction. This emits
 // aggregate counts, never an audit record for each deleted debug event.
 a,e:=tx.ExecContext(ctx,`DELETE FROM ao_history_retention WHERE object_kind='provider_event' AND object_id=?`,maintenanceID(p.id));if e!=nil{return false,e}
 n,e:=a.RowsAffected();if e!=nil{return false,e}
 d,e:=tx.ExecContext(ctx,`DELETE FROM conversation_provider_events WHERE id=? AND provider_event_id='' AND method IN `+historyGCStreams,p.id);if e!=nil{return false,e}
 count,e:=d.RowsAffected();if e!=nil{return false,e};if count!=1{return false,errors.New("gc_candidates_changed")}
 result.AuditDeleted+=int(n);result.Deleted++;return true,nil
}
