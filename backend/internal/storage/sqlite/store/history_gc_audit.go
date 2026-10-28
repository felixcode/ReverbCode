package store

import("context";"database/sql";"errors";"reflect";"time")

func historyGCAuditMetadata(ctx context.Context,db maintenanceDB,id any)(maintenanceRow,error){
 var kind,key,conversation,expired,cutoff string
 e:=db.QueryRowContext(ctx,`SELECT object_kind,object_id,conversation_id,expired_at,cutoff_at FROM ao_history_retention WHERE rowid=?`,id).Scan(&kind,&key,&conversation,&expired,&cutoff)
 if errors.Is(e,sql.ErrNoRows){return nil,nil};if e!=nil{return nil,e};return maintenanceRow{"kind":kind,"key":key,"conversation_id":conversation,"expired_at":expired,"cutoff_at":cutoff},nil
}
func prepareHistoryGCAudits(ctx context.Context,db maintenanceDB,ids []any)([]maintenancePrepared,error){
 plans:=make([]maintenancePrepared,0,len(ids));for _,id:=range ids{row,e:=historyGCAuditMetadata(ctx,db,id);if e!=nil{return nil,e};plans=append(plans,maintenancePrepared{id:id,row:row})};return plans,nil
}
func applyHistoryGCAudit(ctx context.Context,tx *sql.Tx,p maintenancePrepared,cutoff time.Time,r *HistoryMaintenanceResult)(bool,error){
 row,e:=historyGCAuditMetadata(ctx,tx,p.id);if e!=nil{return false,e};if row==nil{return true,nil};if !reflect.DeepEqual(row,p.row){r.Retained["audit_changed"]++;return false,nil}
 var table string
 switch row.text("kind") {case "activity","activity_output":table="conversation_activities";case "message","message_coordination":table="conversation_messages";case "turn":table="conversation_turns";case "session":table="sessions";case "provider_event":r.Retained["audit_provider_event_owned"]++;return true,nil;default:r.Retained["audit_kind_unknown"]++;return true,nil}
 limit:=time.Now().UTC().Add(-48*time.Hour);if cutoff.After(limit){cutoff=limit}
 stamp,e:=maintenanceTime(row.text("expired_at"));if e!=nil{r.Retained["audit_invalid_timestamp"]++;return true,nil};prior,e:=maintenanceTime(row.text("cutoff_at"));if e!=nil||prior.After(stamp){r.Retained["audit_invalid_timestamp"]++;return true,nil}
 if stamp.After(cutoff){r.Retained["audit_within_48h"]++;return true,nil}
 present,e:=maintenanceExists(ctx,tx,`SELECT 1 FROM `+table+` WHERE id=?`,row.text("key"));if e!=nil{return false,e};if present{r.Retained["audit_source_present"]++;return true,nil}
 d,e:=tx.ExecContext(ctx,`DELETE FROM ao_history_retention WHERE rowid=? AND object_kind=? AND object_id=? AND expired_at=? AND cutoff_at=?`,p.id,row.text("kind"),row.text("key"),row.text("expired_at"),row.text("cutoff_at"));if e!=nil{return false,e};n,e:=d.RowsAffected();if e!=nil{return false,e};if n!=1{return false,sql.ErrNoRows};r.AuditDeleted++;r.Deleted++;return true,nil
}
