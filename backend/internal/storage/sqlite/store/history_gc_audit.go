package store

import("context";"database/sql";"errors";"reflect";"time";"encoding/json";"unicode/utf8")

type historyAuditKey [2]string
func(k historyAuditKey) String()string{v,_:=json.Marshal([2]string(k));return string(v)}
func historyGCAuditKeys(ctx context.Context,db maintenanceDB,cursor string,limit int)([]any,error){
 var key historyAuditKey
 if cursor!=""{var parts []string;if len(cursor)>8192||json.Unmarshal([]byte(cursor),&parts)!=nil||len(parts)!=2{return nil,errors.New("invalid_audit_tuple_cursor")};key=historyAuditKey{parts[0],parts[1]}}
 rows,e:=db.QueryContext(ctx,`SELECT object_kind,object_id FROM ao_history_retention WHERE (object_kind,object_id)>(?,?) ORDER BY object_kind,object_id LIMIT ?`,key[0],key[1],limit);if e!=nil{return nil,e};defer rows.Close()
 ids:=[]any{};for rows.Next(){var next historyAuditKey;if e=rows.Scan(&next[0],&next[1]);e!=nil{return nil,e};if !utf8.ValidString(next[0])||!utf8.ValidString(next[1]){return nil,errors.New("audit_key_invalid_utf8")};ids=append(ids,next)};return ids,rows.Err()
}
func historyGCAuditMetadata(ctx context.Context,db maintenanceDB,id any)(maintenanceRow,error){
 pair,ok:=id.(historyAuditKey);if !ok{return nil,errors.New("invalid_audit_tuple_key")}
 var kind,key,conversation,expired,cutoff string
 e:=db.QueryRowContext(ctx,`SELECT object_kind,object_id,conversation_id,expired_at,cutoff_at FROM ao_history_retention WHERE object_kind=? AND object_id=?`,pair[0],pair[1]).Scan(&kind,&key,&conversation,&expired,&cutoff)
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
 d,e:=tx.ExecContext(ctx,`DELETE FROM ao_history_retention WHERE object_kind=? AND object_id=? AND expired_at=? AND cutoff_at=?`,row.text("kind"),row.text("key"),row.text("expired_at"),row.text("cutoff_at"));if e!=nil{return false,e};n,e:=d.RowsAffected();if e!=nil{return false,e};if n!=1{return false,sql.ErrNoRows};r.AuditDeleted++;r.Deleted++;return true,nil
}
