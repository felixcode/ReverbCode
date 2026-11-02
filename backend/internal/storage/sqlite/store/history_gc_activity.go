package store

import (
 "context"
 "database/sql"
 "reflect"
 "strings"
 "errors"
 "time"
 "github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func historyActivityGCClass(kind,status,providerItem,request string) bool {
 if providerItem!="" || request!="" { return false }
 switch status { case "completed","failed","resolved": default:return false }
 switch kind {case "command","file_change","reasoning","usage","error":return true};return false
}

// Exactly one native turn and its retained expiry marker prevent a late replay
// from constructing this class of anonymous timeline entry after row removal.
func historyActivityGCExpiredTurn(ctx context.Context,db maintenanceDB,conversation,native string,cutoff time.Time)(string,error){
 if native=="" {return "",nil}
 rows,e:=db.QueryContext(ctx,`SELECT id FROM conversation_turns WHERE conversation_id=? AND provider_turn_id=? AND provider_turn_id<>'' LIMIT 2`,conversation,native);if e!=nil{return "",e}
 var ids []string;for rows.Next(){var id string;if e=rows.Scan(&id);e!=nil{rows.Close();return "",e};ids=append(ids,id)};e=rows.Err();rows.Close();if e!=nil||len(ids)!=1{return "",e}
 turn,e:=maintenanceTurn(ctx,db,ids[0]);if e!=nil||turn==nil{return "",e};if !maintenanceTerminal(turn.text("state")){return "",nil}
 old,e:=maintenanceOld(turn.text("completed_at"),cutoff,true);if e!=nil||!old{return "",nil}
 yes,e:=maintenanceExists(ctx,db,`SELECT 1 FROM ao_history_retention WHERE object_kind='turn' AND object_id=? AND conversation_id=?`,ids[0],conversation);if e!=nil||!yes{return "",e};return ids[0],nil
}

func historyActivityGCEligible(ctx context.Context,db maintenanceDB,row maintenanceRow,cutoff time.Time)(bool,error){
 if row==nil||!historyActivityGCClass(row.text("kind"),row.text("status"),row.text("provider_item_id"),row.text("request_id")){return false,nil}
 limit:=time.Now().UTC().Add(-48*time.Hour);if cutoff.After(limit){cutoff=limit}
 for _,k:=range []string{"created_at","updated_at"}{old,e:=maintenanceOld(row.text(k),cutoff,true);if e!=nil||!old{return false,nil}}
 if row.text("turn_id")==""{return false,nil}
 turn,e:=maintenanceTurn(ctx,db,row.text("turn_id"));if e!=nil||turn==nil{return false,e}
 var native string;if e=db.QueryRowContext(ctx,`SELECT provider_turn_id FROM conversation_turns WHERE id=?`,row.text("turn_id")).Scan(&native);e!=nil{return false,e}
 id,e:=historyActivityGCExpiredTurn(ctx,db,row.text("conversation_id"),native,cutoff);if e!=nil||id!=row.text("turn_id"){return false,e}
 return maintenanceEligibleWithProof(ctx,db,row,"activity",cutoff,nil)
}

func historyActivityGCMetadata(ctx context.Context,db maintenanceDB,id any)(maintenanceRow,error){
 return maintenanceMetadata(ctx,db,"conversation_activities","activity",id)
}

// Native-identified rows are always retained. Read only the early identity
// column once for this PK window, avoiding overflow-bearing metadata tails.
func prepareHistoryGCActivities(ctx context.Context,db maintenanceDB,ids []any)([]maintenancePrepared,error){
 plans:=make([]maintenancePrepared,0,len(ids));if len(ids)==0{return plans,nil}
 marks:=strings.TrimSuffix(strings.Repeat("?,",len(ids)),",")
 rows,e:=db.QueryContext(ctx,`SELECT id,typeof(provider_item_id),CASE WHEN typeof(provider_item_id)='text' THEN provider_item_id<>'' ELSE 0 END FROM conversation_activities WHERE id IN (`+marks+`)`,ids...);if e!=nil{return nil,e}
 keep:=map[string]string{}
 for rows.Next(){var id,kind string;var native bool;if e=rows.Scan(&id,&kind,&native);e!=nil{break};if kind!="text"{keep[id]="activity_identity_type_unknown"}else if native{keep[id]="activity_native_or_request_identity"}}
 if e==nil{e=rows.Err()};closeErr:=rows.Close();if e==nil{e=closeErr};if e!=nil{return nil,e}
 for _,id:=range ids {
  if reason:=keep[maintenanceID(id)];reason!=""{plans=append(plans,maintenancePrepared{id:id,skip:reason});continue}
  row,e:=historyActivityGCMetadata(ctx,db,id);if e!=nil{var reason maintenanceRetained;if errors.As(e,&reason){plans=append(plans,maintenancePrepared{id:id,skip:string(reason)});continue};return nil,e};plans=append(plans,maintenancePrepared{id:id,row:row})
 };return plans,nil
}

func historyActivityGCRetainedReason(row maintenanceRow,cutoff time.Time)string{
 if row.text("status")=="pending"{return "activity_pending_request"}
 if row.text("provider_item_id")!=""||row.text("request_id")!=""{return "activity_native_or_request_identity"}
 if !historyActivityGCClass(row.text("kind"),row.text("status"),"",""){return "activity_control_or_nonterminal"}
 limit:=time.Now().UTC().Add(-48*time.Hour);if cutoff.After(limit){cutoff=limit}
 for _,key:=range []string{"created_at","updated_at"}{old,e:=maintenanceOld(row.text(key),cutoff,true);if e!=nil{return "activity_invalid_timestamp"};if !old{return "activity_within_48h"}}
 if row.text("turn_id")==""{return "activity_turn_unattributed"};return "activity_turn_expiry_unproven"
}

func applyHistoryGCActivity(ctx context.Context,tx *sql.Tx,p maintenancePrepared,cutoff time.Time,r *HistoryMaintenanceResult)(bool,error){
 if p.skip!="" {r.Retained[p.skip]++;return true,nil}
 current,e:=historyActivityGCMetadata(ctx,tx,p.id);if e!=nil{return false,e};if current==nil{return true,nil}
 if !reflect.DeepEqual(current,p.row){r.Retained["concurrent_body_or_owner_change"]++;return false,nil}
 eligible,e:=historyActivityGCEligible(ctx,tx,current,cutoff);if e!=nil{var reason maintenanceRetained;if errors.As(e,&reason){r.Retained[string(reason)]++;return true,nil};return false,e};if !eligible{r.Retained[historyActivityGCRetainedReason(current,cutoff)]++;return true,nil}
 var bodyBytes int64
 if e=tx.QueryRowContext(ctx,`SELECT octet_length(summary)+octet_length(detail_json)+octet_length(command_output)+octet_length(streamed_text) FROM conversation_activities WHERE id=?`,p.id).Scan(&bodyBytes);e!=nil{return false,e}
 a,e:=tx.ExecContext(ctx,`DELETE FROM ao_history_retention WHERE object_kind IN ('activity','activity_output') AND object_id=? AND conversation_id=?`,p.id,current.text("conversation_id"));if e!=nil{return false,e};n,e:=a.RowsAffected();if e!=nil{return false,e}
 d,e:=tx.ExecContext(ctx,`DELETE FROM conversation_activities WHERE id=? AND conversation_id=? AND revision=? AND provider_item_id='' AND request_id=''`,p.id,current.text("conversation_id"),current.integer("revision"));if e!=nil{return false,e};deleted,e:=d.RowsAffected();if e!=nil{return false,e};if deleted!=1{return false,sql.ErrNoRows}
 r.Deleted++;r.AuditDeleted+=int(n);r.LogicalBytes+=bodyBytes;return true,nil
}

func(s *Store) historyActivityGCReplayExpired(ctx context.Context,conversation,native string,a domain.ConversationActivity)(bool,error){
 if !historyActivityGCClass(string(a.Kind),string(a.Status),a.ProviderItemID,a.RequestID){return false,nil}
 id,e:=historyActivityGCExpiredTurn(ctx,s.retentionDB(ctx),conversation,native,time.Now().UTC().Add(-48*time.Hour));return id!="",e
}
