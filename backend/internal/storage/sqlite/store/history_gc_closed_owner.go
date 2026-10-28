package store

import (
 "context"
 "database/sql"
 "errors"
 "reflect"
 "strings"
 "time"
)

type closedHistoryProof struct { allowed bool; reason string }
type closedHistoryCache struct { owners map[string]closedHistoryProof; anchors map[string]int64; activityAnchors map[string]string }
func newClosedHistoryCache()*closedHistoryCache{return &closedHistoryCache{map[string]closedHistoryProof{},map[string]int64{},map[string]string{}}}

// This first release is strictly the stopped-daemon, sealed-backup path. The
// wire API cannot set the unexported recovery coverage context.
func closedHistoryOwner(ctx context.Context,db maintenanceDB,conversation string,cutoff time.Time,cache *closedHistoryCache)(closedHistoryProof,error){
 if v,ok:=cache.owners[conversation];ok{return v,nil}
 keep:=func(reason string)(closedHistoryProof,error){v:=closedHistoryProof{false,reason};cache.owners[conversation]=v;return v,nil}
 _,ok:=ctx.Value(historyBodyCoverageKey{}).(historyBodyCoverage);if !ok{return keep("closed_owner_offline_proof_required")}
 var scope,session string
 e:=db.QueryRowContext(ctx,`SELECT scope,COALESCE(session_id,'') FROM conversations WHERE id=?`,conversation).Scan(&scope,&session)
 if errors.Is(e,sql.ErrNoRows){return keep("closed_owner_missing")};if e!=nil{return closedHistoryProof{},e}
 if scope!="session"||session==""{return keep("closed_owner_review_or_unknown_scope")}
 tail,e:=historyGCProtected(ctx,db,conversation);if e!=nil{return closedHistoryProof{},e};if tail.unknown||!tail.at.IsZero(){return keep("closed_owner_pending_delivery_or_turn")}
 yes,e:=maintenanceClosedOwner(ctx,db,conversation);if e!=nil{return closedHistoryProof{},e};if !yes{return keep("closed_owner_not_closed")}
 rows,e:=db.QueryContext(ctx,`SELECT session_id FROM conversation_branches WHERE conversation_id=? UNION SELECT session_id FROM conversations WHERE id=? UNION SELECT current_session_id FROM conversations WHERE id=?`,conversation,conversation,conversation);if e!=nil{return closedHistoryProof{},e}
 var owners []string
 for rows.Next(){var owner sql.NullString;if e=rows.Scan(&owner);e!=nil{break};if owner.Valid&&owner.String!=""{owners=append(owners,owner.String)}}
 if e==nil{e=rows.Err()};ce:=rows.Close();if e==nil{e=ce};if e!=nil{return closedHistoryProof{},e}
 for _,owner:=range owners{
  var terminated,pinned,unsettled int
  if e=db.QueryRowContext(ctx,`SELECT is_terminated,is_pinned,conversation_checkpoint_unsettled FROM sessions WHERE id=?`,owner).Scan(&terminated,&pinned,&unsettled);e!=nil{if errors.Is(e,sql.ErrNoRows){return keep("closed_owner_missing")};return closedHistoryProof{},e}
  if terminated!=1||pinned!=0||unsettled!=0{return keep("closed_owner_current_or_checkpoint")}
  yes,e=maintenanceSessionClosed(ctx,db,owner);if e!=nil{return closedHistoryProof{},e};if !yes{return keep("closed_owner_session_pending")}

 }
 proof:=closedHistoryProof{true,""};cache.owners[conversation]=proof;return proof,nil
}

func closedHistoryMetadata(ctx context.Context,db maintenanceDB,stage string,id any)(maintenanceRow,error){
 if stage=="gc:closed-raw"{return maintenanceReadColumns(ctx,db,"conversation_provider_events",id,`id,conversation_id,session_id,provider_event_id,method,branch_id,CAST(received_at AS TEXT) AS received_at`)}
 return maintenanceMetadata(ctx,db,"conversation_activities","activity",id)
}
func prepareClosedHistory(ctx context.Context,db maintenanceDB,stage string,ids []any,cutoff time.Time)([]maintenancePrepared,error){
 plans:=make([]maintenancePrepared,0,len(ids));cache:=newClosedHistoryCache()
 for _,id:=range ids{
  row,e:=closedHistoryMetadata(ctx,db,stage,id);if e!=nil{return nil,e}
  p:=maintenancePrepared{id:id,row:row}
  if row!=nil{proof,e:=closedHistoryOwner(ctx,db,row.text("conversation_id"),cutoff,cache);if e!=nil{return nil,e};if !proof.allowed{p.skip=proof.reason}}
  plans=append(plans,p)
 };return plans,nil
}
func closedHistoryAuthAnchor(ctx context.Context,db maintenanceDB,conv,branch string,cache *closedHistoryCache)(int64,error){
 key:=conv+"\x00"+branch;if id,ok:=cache.anchors[key];ok{return id,nil}
 var id int64
 e:=db.QueryRowContext(ctx,`SELECT e.id
 FROM conversation_provider_events e
 JOIN conversations c ON c.id=e.conversation_id
 JOIN conversation_branches b ON b.id=e.branch_id AND b.conversation_id=e.conversation_id
 JOIN conversation_turns t ON t.conversation_id=e.conversation_id AND t.branch_id=e.branch_id
   AND t.provider_turn_id=CASE WHEN json_valid(e.payload_json) THEN json_extract(e.payload_json,'$.providerTurnId') ELSE NULL END
 WHERE e.conversation_id=? AND e.branch_id=? AND e.method='turn.completed'
   AND t.state='completed' AND t.rolled_back_at IS NULL AND t.provider_turn_id<>''
   AND t.controller_generation=COALESCE((SELECT controller_generation FROM review WHERE id=c.current_review_id),(SELECT controller_generation FROM sessions WHERE id=c.current_session_id))
   AND CASE WHEN json_valid(e.payload_json) THEN json_type(e.payload_json,'$.providerTurnId')='text'
     AND json_extract(e.payload_json,'$.turnState')='completed'
     AND json_extract(e.payload_json,'$.error') IS NULL
     AND COALESCE(json_extract(e.payload_json,'$.providerConversationId'),'') IN ('',b.provider_conversation_id)
     ELSE 0 END
 ORDER BY t.completed_at DESC,e.id DESC LIMIT 1`,conv,branch).Scan(&id)
 if errors.Is(e,sql.ErrNoRows){e=nil};if e==nil{cache.anchors[key]=id};return id,e
}
func applyClosedHistory(ctx context.Context,tx *sql.Tx,p maintenancePrepared,stage string,cutoff time.Time,r *HistoryMaintenanceResult)(bool,error){
 if p.skip!=""{r.Retained[p.skip]++;return true,nil}
 if r.closedHistoryWriter==nil{r.closedHistoryWriter=newClosedHistoryCache()}
 row,e:=closedHistoryMetadata(ctx,tx,stage,p.id);if e!=nil{return false,e};if row==nil{return true,nil}
 if !reflect.DeepEqual(row,p.row){r.Retained["closed_owner_metadata_changed"]++;return false,nil}
 proof,e:=closedHistoryOwner(ctx,tx,row.text("conversation_id"),cutoff,r.closedHistoryWriter);if e!=nil{return false,e};if !proof.allowed{r.Retained[proof.reason]++;return true,nil}
 conv:=row.text("conversation_id");kind:="provider_event";table:="conversation_provider_events"
 if stage=="gc:closed-raw"{
  if reason:=historyBodyCoverageReason(ctx,row,"gc:raw");reason!=""{r.Retained[reason]++;return true,nil}
  old,e:=maintenanceOld(row.text("received_at"),cutoff,true);if e!=nil||!old{r.Retained["closed_raw_recent_or_unknown"]++;return true,nil}
  method:=row.text("method");if method=="queue.preserved_recovered"||method=="turn.transport_recovered"||strings.HasPrefix(row.text("provider_event_id"),"live-turn-recovery:")||strings.HasPrefix(row.text("provider_event_id"),"preserved-queue-recovery:"){r.Retained["closed_raw_queue_recovery_proof"]++;return true,nil}
  if row.text("branch_id")==""{r.Retained["closed_raw_unknown_branch"]++;return true,nil}
  if method=="turn.completed"{id,e:=closedHistoryAuthAnchor(ctx,tx,conv,row.text("branch_id"),r.closedHistoryWriter);if e!=nil{return false,e};if id==row.integer("id"){r.Retained["closed_raw_auth_anchor"]++;return true,nil}}
 }else{
  kind="activity";table="conversation_activities"
  key:=conv+"\x00"+row.text("branch_id");anchor,known:=r.closedHistoryWriter.activityAnchors[key]
  if !known{e=tx.QueryRowContext(ctx,`SELECT id FROM conversation_activities WHERE conversation_id=? AND branch_id=? ORDER BY sequence DESC LIMIT 1`,conv,row.text("branch_id")).Scan(&anchor);if e!=nil&&!errors.Is(e,sql.ErrNoRows){return false,e};r.closedHistoryWriter.activityAnchors[key]=anchor}
  if maintenanceID(p.id)==anchor{r.Retained["closed_activity_sequence_anchor"]++;return true,nil}
  switch row.text("status"){case "completed","failed","resolved","cancelled":default:r.Retained["closed_activity_nonterminal"]++;return true,nil}
  if row.text("kind")=="approval"||row.text("kind")=="user_input"{r.Retained["closed_activity_request_identity"]++;return true,nil}
  for _,k:=range []string{"created_at","updated_at"}{old,e:=maintenanceOld(row.text(k),cutoff,true);if e!=nil||!old{r.Retained["closed_activity_recent_or_unknown"]++;return true,nil}}
  if reason:=historyBodyCoverageReason(ctx,row,"activity");reason!=""{r.Retained[reason]++;return true,nil}
 }
 // Derived AHR records are removed exactly with their already-backed-up source
 // even when a later compaction produced the marker after the freeze time.
 q:=`DELETE FROM ao_history_retention WHERE object_kind='provider_event' AND object_id=? AND conversation_id=?`
 if kind=="activity"{q=`DELETE FROM ao_history_retention WHERE object_kind IN ('activity','activity_output') AND object_id=? AND conversation_id=?`}
 a,e:=tx.ExecContext(ctx,q,maintenanceID(p.id),conv);if e!=nil{return false,e};n,e:=a.RowsAffected();if e!=nil{return false,e}
 d,e:=tx.ExecContext(ctx,`DELETE FROM "`+table+`" WHERE id=? AND conversation_id=?`,p.id,conv);if e!=nil{return false,e};count,e:=d.RowsAffected();if e!=nil{return false,e};if count!=1{return false,errors.New("closed_owner_delete_changed")}
 r.Deleted++;r.AuditDeleted+=int(n);return true,nil
}
