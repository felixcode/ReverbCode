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
type closedHistoryCache struct { owners map[string]closedHistoryProof; sessions map[string]closedHistoryProof; anchors map[string]int64; activityAnchors map[string]string }
func newClosedHistoryCache()*closedHistoryCache{return &closedHistoryCache{map[string]closedHistoryProof{},map[string]closedHistoryProof{},map[string]int64{},map[string]string{}}}

type closedHistoryOnlineGuardKey struct{}
// SetClosedOwnerMaintenanceGuard is wired only by the daemon, not the request.
// The guard must be memory-only, nonblocking, and conservative for unknowns.
func(s *Store) SetClosedOwnerMaintenanceGuard(guard func(string)bool){if guard!=nil{s.closedOwnerMaintenanceGuard.Store(guard)}}
func(s *Store) ClosedOwnerMaintenanceConfigured()bool{return s.closedOwnerMaintenanceGuard.Load()!=nil}

// Offline requests carry sealed coverage; online requests require the daemon's
// live ownership guard. A same-UID HTTP client cannot supply either authority.
func closedHistorySessionProof(ctx context.Context,db maintenanceDB,owner string,cache *closedHistoryCache)(closedHistoryProof,error){
 if p,ok:=cache.sessions[owner];ok{return p,nil}
 keep:=func(reason string)(closedHistoryProof,error){p:=closedHistoryProof{false,reason};cache.sessions[owner]=p;return p,nil}
 _,offline:=ctx.Value(historyBodyCoverageKey{}).(historyBodyCoverage);guard,online:=ctx.Value(closedHistoryOnlineGuardKey{}).(func(string)bool)
 if !offline&&(!online||!guard(owner)){return keep("closed_owner_live_or_unknown_controller")}
 var terminated,pinned,unsettled int
 e:=db.QueryRowContext(ctx,`SELECT is_terminated,is_pinned,conversation_checkpoint_unsettled FROM sessions WHERE id=?`,owner).Scan(&terminated,&pinned,&unsettled)
 if errors.Is(e,sql.ErrNoRows){return keep("closed_owner_missing")};if e!=nil{return closedHistoryProof{},e}
 if terminated!=1||pinned!=0||unsettled!=0{return keep("closed_owner_current_or_checkpoint")}
 pendingUsage,e:=maintenanceExists(ctx,db,`SELECT 1 FROM usage_bindings WHERE session_id=? AND state NOT IN ('complete','partial') LIMIT 1`,owner);if e!=nil{return closedHistoryProof{},e};if pendingUsage{return keep("closed_owner_native_usage_pending")}
 // Review existence is not activity. Require its own terminal/post proof,
 // without recursing through closedHistoryOwner or the old scope-only helper.
 reviewRows,e:=db.QueryContext(ctx,`SELECT id FROM review WHERE session_id=?`,owner);if e!=nil{return closedHistoryProof{},e}
 var reviewIDs []string;for reviewRows.Next(){var id string;if e=reviewRows.Scan(&id);e!=nil{break};reviewIDs=append(reviewIDs,id)};if e==nil{e=reviewRows.Err()};ce:=reviewRows.Close();if e==nil{e=ce};if e!=nil{return closedHistoryProof{},e}
 for _,id:=range reviewIDs{proof,e:=closedHistoryReviewProof(ctx,db,id);if e!=nil{return closedHistoryProof{},e};if !proof.allowed{return keep(proof.reason)}}
 pending,e:=maintenancePendingSwitch(ctx,db,owner);if e!=nil{return closedHistoryProof{},e};if pending{return keep("closed_owner_pending_switch")}
 for _,q:=range []string{`SELECT 1 FROM conversation_turns WHERE handled_by_session_id=? AND state IN ('queued','running') LIMIT 1`,`SELECT 1 FROM review_run WHERE session_id=? AND status NOT IN ('complete','cancelled','failed') LIMIT 1`}{
  pending,e=maintenanceExists(ctx,db,q,owner);if e!=nil{return closedHistoryProof{},e};if pending{return keep("closed_owner_unfinished_turn_or_review")}
 }
 // Inspect pending state in every owned conversation without recursively
 // treating the mere existence of a historical review as live ownership.
 rows,e:=db.QueryContext(ctx,`SELECT id FROM conversations WHERE session_id=? OR current_session_id=?
 UNION SELECT conversation_id FROM conversation_branches WHERE session_id=?
 UNION SELECT c.id FROM conversations c JOIN review r ON r.id=c.review_id OR r.id=c.current_review_id WHERE r.session_id=?
 UNION SELECT b.conversation_id FROM conversation_branches b JOIN review r ON r.id=b.review_id WHERE r.session_id=?`,owner,owner,owner,owner,owner);if e!=nil{return closedHistoryProof{},e}
 var ids []string;for rows.Next(){var id string;if e=rows.Scan(&id);e!=nil{break};ids=append(ids,id)};if e==nil{e=rows.Err()};ce=rows.Close();if e==nil{e=ce};if e!=nil{return closedHistoryProof{},e}
 for _,id:=range ids{tail,e:=historyGCProtected(ctx,db,id);if e!=nil{return closedHistoryProof{},e};if tail.unknown||!tail.at.IsZero(){return keep("closed_owner_pending_delivery_or_turn")}}
 p:=closedHistoryProof{true,""};cache.sessions[owner]=p;return p,nil
}
func closedHistoryReviewProof(ctx context.Context,db maintenanceDB,id string)(closedHistoryProof,error){
 var owner string;e:=db.QueryRowContext(ctx,`SELECT session_id FROM review WHERE id=?`,id).Scan(&owner)
 if errors.Is(e,sql.ErrNoRows){return closedHistoryProof{false,"closed_review_missing"},nil};if e!=nil{return closedHistoryProof{},e}
 for _,q:=range []string{`SELECT 1 FROM pr WHERE session_id=? AND is_merged=0 AND is_closed=0 LIMIT 1`,`SELECT 1 FROM usage_bindings WHERE session_id=? AND state NOT IN ('complete','partial') LIMIT 1`}{
  yes,e:=maintenanceExists(ctx,db,q,owner);if e!=nil{return closedHistoryProof{},e};if yes{return closedHistoryProof{false,"closed_review_open_pr_or_native_usage"},nil}
 }
 rows,e:=db.QueryContext(ctx,`SELECT status,COALESCE(verdict,''),COALESCE(github_review_id,'') FROM review_run WHERE review_id=?`,id);if e!=nil{return closedHistoryProof{},e};defer rows.Close();count:=0
 for rows.Next(){var state,verdict,post string;if e=rows.Scan(&state,&verdict,&post);e!=nil{return closedHistoryProof{},e};count++
  if state!="complete"&&state!="cancelled"&&state!="failed"{return closedHistoryProof{false,"closed_review_unfinished"},nil}
  if state=="complete"&&(post==""||(verdict!="approved"&&verdict!="changes_requested")){return closedHistoryProof{false,"closed_review_post_unproven"},nil}
 };if e=rows.Err();e!=nil{return closedHistoryProof{},e};if count==0{return closedHistoryProof{false,"closed_review_no_lifecycle_proof"},nil};return closedHistoryProof{true,""},nil
}
func closedHistoryOwner(ctx context.Context,db maintenanceDB,conversation string,cutoff time.Time,cache *closedHistoryCache)(closedHistoryProof,error){
 if v,ok:=cache.owners[conversation];ok{return v,nil}
 keep:=func(reason string)(closedHistoryProof,error){v:=closedHistoryProof{false,reason};cache.owners[conversation]=v;return v,nil}
 _,offline:=ctx.Value(historyBodyCoverageKey{}).(historyBodyCoverage);_,online:=ctx.Value(closedHistoryOnlineGuardKey{}).(func(string)bool);if !offline&&!online{return keep("closed_owner_lifecycle_proof_required")}
 var scope,session,reviewID string
 e:=db.QueryRowContext(ctx,`SELECT scope,COALESCE(session_id,''),COALESCE(review_id,'') FROM conversations WHERE id=?`,conversation).Scan(&scope,&session,&reviewID)
 if errors.Is(e,sql.ErrNoRows){return keep("closed_owner_missing")};if e!=nil{return closedHistoryProof{},e}
 if (scope!="session"&&scope!="review")||(scope=="session"&&session=="")||(scope=="review"&&reviewID==""){return keep("closed_owner_unknown_scope")}
 tail,e:=historyGCProtected(ctx,db,conversation);if e!=nil{return closedHistoryProof{},e};if tail.unknown||!tail.at.IsZero(){return keep("closed_owner_pending_delivery_or_turn")}
 if scope=="review"{
  rows,e:=db.QueryContext(ctx,`SELECT review_id FROM conversations WHERE id=? UNION SELECT current_review_id FROM conversations WHERE id=? UNION SELECT review_id FROM conversation_branches WHERE conversation_id=?`,conversation,conversation,conversation);if e!=nil{return closedHistoryProof{},e}
  var ids []string;for rows.Next(){var id sql.NullString;if e=rows.Scan(&id);e!=nil{break};if id.Valid&&id.String!=""{ids=append(ids,id.String)}};if e==nil{e=rows.Err()};ce:=rows.Close();if e==nil{e=ce};if e!=nil{return closedHistoryProof{},e}
  for _,id:=range ids{proof,e:=closedHistoryReviewProof(ctx,db,id);if e!=nil{return closedHistoryProof{},e};if !proof.allowed{return keep(proof.reason)}}
 }
 rows,e:=db.QueryContext(ctx,`SELECT session_id FROM conversation_branches WHERE conversation_id=?
 UNION SELECT session_id FROM conversations WHERE id=? UNION SELECT current_session_id FROM conversations WHERE id=?
 UNION SELECT r.session_id FROM review r WHERE r.id IN (SELECT review_id FROM conversations WHERE id=? UNION SELECT current_review_id FROM conversations WHERE id=? UNION SELECT review_id FROM conversation_branches WHERE conversation_id=?)`,conversation,conversation,conversation,conversation,conversation,conversation);if e!=nil{return closedHistoryProof{},e}
 var owners []string;for rows.Next(){var owner sql.NullString;if e=rows.Scan(&owner);e!=nil{break};if owner.Valid&&owner.String!=""{owners=append(owners,owner.String)}};if e==nil{e=rows.Err()};ce:=rows.Close();if e==nil{e=ce};if e!=nil{return closedHistoryProof{},e};if len(owners)==0{return keep("closed_owner_missing")}
 for _,owner:=range owners{proof,e:=closedHistorySessionProof(ctx,db,owner,cache);if e!=nil{return closedHistoryProof{},e};if !proof.allowed{return keep(proof.reason)}}
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
