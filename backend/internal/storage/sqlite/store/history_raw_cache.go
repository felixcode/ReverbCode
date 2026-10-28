package store

import("context";"database/sql";"encoding/json";"errors";"reflect";"strings";"time")

const rawCacheMethods = `('message.delta','reasoning.delta','command.output.delta','activity.text','activity.started','activity.completed','usage','account.rateLimits','message.completed','turn.diff')`
const rawCacheInsert = `INSERT INTO conversation_provider_events(id,conversation_id,session_id,provider_event_id,method,payload_json,received_at,branch_id,review_id) VALUES(?,?,?,?,?,?,?,?,?)`
const rawCacheMetadata = `id,conversation_id,session_id,provider_event_id,method,CAST(received_at AS TEXT),branch_id,review_id`
type rawCachePlan struct{ID int64;Conversation,Session,Event,Method,Received,Branch string;Review sql.NullString;Old,Small string;Audit bool}
func(p *rawCachePlan) values(payload string)[]any{var review any;if p.Review.Valid{review=p.Review.String};return []any{p.ID,p.Conversation,p.Session,p.Event,p.Method,payload,p.Received,p.Branch,review}}
func rawCacheSmall(ctx context.Context,payload string)(string,error){
 value,e:=maintenanceJSONContext(ctx,payload);if e!=nil{return "",e};obj,ok:=value.(map[string]any);if !ok{return "",errors.New("raw_cache_not_object")}
 small:=map[string]any{"_rawCache":1}
 for _,key:=range []string{"providerTurnId","providerConversationId"}{if v,exists:=obj[key];exists{switch v.(type){case nil,string,json.Number,bool:small[key]=v;default:return "",errors.New("raw_cache_correlation_type")}}}
 encoded,e:=json.Marshal(small);return string(encoded),e
}
func rawCacheIsSmall(obj map[string]any)bool{
 n,ok:=obj["_rawCache"].(json.Number);if !ok||string(n)!="1"{return false}
 for k,v:=range obj{if k=="_rawCache"{continue};if k!="providerTurnId"&&k!="providerConversationId"{return false};switch v.(type){case nil,string,json.Number,bool:default:return false}}
 return true
}
func rawCacheSchema(ctx context.Context,db maintenanceDB)error{
 expected:=[]string{"id","conversation_id","session_id","provider_event_id","method","payload_json","received_at","branch_id","review_id"}
 rows,e:=db.QueryContext(ctx,`PRAGMA table_info(conversation_provider_events)`);if e!=nil{return e};var names []string
 for rows.Next(){var pos,nn,pk int;var name,typ string;var def any;if e=rows.Scan(&pos,&name,&typ,&nn,&def,&pk);e!=nil{break};names=append(names,name)};if e==nil{e=rows.Err()};rows.Close();if e!=nil{return e};if !reflect.DeepEqual(names,expected){return errors.New("raw_cache_unknown_columns")}
 rows,e=db.QueryContext(ctx,`SELECT name,sql FROM sqlite_schema WHERE type='trigger' AND tbl_name='conversation_provider_events'`);if e!=nil{return e}
 known:=`CREATE TRIGGER conversation_provider_events_branch_insert AFTER INSERT ON conversation_provider_events WHEN NEW.branch_id = '' BEGIN UPDATE conversation_provider_events SET branch_id = (SELECT active_branch_id FROM conversations WHERE id = NEW.conversation_id) WHERE id = NEW.id; END`
 normalize:=func(v string)string{return strings.Join(strings.Fields(strings.TrimSuffix(strings.TrimSpace(v),";"))," ")};count:=0
 for rows.Next(){var name,ddl string;if e=rows.Scan(&name,&ddl);e!=nil{break};count++;if name!="conversation_provider_events_branch_insert"||normalize(ddl)!=normalize(known){e=errors.New("raw_cache_unknown_trigger");break}};if e==nil{e=rows.Err()};rows.Close();if e!=nil{return e};if count!=1{return errors.New("raw_cache_trigger_missing")}
 rows,e=db.QueryContext(ctx,`SELECT name FROM sqlite_schema WHERE type='table'`);if e!=nil{return e};var tables []string;for rows.Next(){var n string;if e=rows.Scan(&n);e!=nil{break};tables=append(tables,n)};if e==nil{e=rows.Err()};rows.Close();if e!=nil{return e}
 for _,name:=range tables{r,x:=db.QueryContext(ctx,`PRAGMA foreign_key_list(`+maintenanceQuoted(name)+`)`);if x!=nil{return x};for r.Next(){var id,seq int;var target,from,to,onUpdate,onDelete,match string;if x=r.Scan(&id,&seq,&target,&from,&to,&onUpdate,&onDelete,&match);x!=nil{break};if target=="conversation_provider_events"{x=errors.New("raw_cache_inbound_reference");break}};if x==nil{x=r.Err()};r.Close();if x!=nil{return x}}
 return nil
}
func prepareRawCacheRows(ctx context.Context,db maintenanceDB,ids []any,o HistoryMaintenanceOptions,cutoff time.Time)([]maintenancePrepared,error){
 if e:=rawCacheSchema(ctx,db);e!=nil{return nil,e};marks:=strings.TrimSuffix(strings.Repeat("?,",len(ids)),",")
 q:=`SELECT `+rawCacheMetadata+`,COALESCE(octet_length(payload_json),0),typeof(conversation_id)='text' AND typeof(session_id)='text' AND typeof(provider_event_id)='text' AND typeof(method)='text' AND typeof(received_at)='text' AND typeof(branch_id)='text' AND typeof(payload_json)='text' AND (review_id IS NULL OR typeof(review_id)='text'),EXISTS(SELECT 1 FROM ao_history_retention r WHERE r.object_kind='provider_event' AND r.object_id=CAST(e.id AS TEXT)) FROM conversation_provider_events e WHERE id IN (`+marks+`) ORDER BY id`
 rows,e:=db.QueryContext(ctx,q,ids...);if e!=nil{return nil,e};var plans []maintenancePrepared;var eligible []any;var used int64;protection:=map[string]historyGCProtection{}
 for rows.Next(){p:=&rawCachePlan{};var size int64;var typesOK bool;if e=rows.Scan(&p.ID,&p.Conversation,&p.Session,&p.Event,&p.Method,&p.Received,&p.Branch,&p.Review,&size,&typesOK,&p.Audit);e!=nil{break};plan:=maintenancePrepared{id:p.ID,rawCache:p};reason:=""
  if !historyRawBodyCanExpire(p.Method){reason="raw_cache_method_kept"}else if !typesOK{reason="raw_cache_metadata_type"}else if p.Branch==""{reason="raw_cache_empty_branch"}else if size>o.MaxRowBytes{reason="row_size_limit"}else if x:=historyBodyCoverageReason(ctx,maintenanceRow{"id":p.ID},"raw");x!=""{reason=x}else{
   stamp,x:=maintenanceTime(p.Received);if x!=nil{reason="invalid_timestamp"}else if stamp.After(cutoff){reason="within_raw_cache_window"}
  }
  plan.skip=reason;plan.size=size;if reason==""{if used+size>o.BatchBytes{break};used+=size;eligible=append(eligible,p.ID)};plans=append(plans,plan)
 };if e==nil{e=rows.Err()};rows.Close();if e!=nil{return nil,e}
 // Fresh pending evidence is only a scoped tail, never an entire live owner.
 for i:=range plans{p:=plans[i].rawCache;if plans[i].skip!=""{continue};reason,x:=historyGCEligible(ctx,db,historyGCRow{conversation:p.Conversation,received:p.Received},HistoryGCOptions{Cutoff:cutoff},protection);if x!=nil{return nil,x};if reason!=""{plans[i].skip=reason}}
 if len(eligible)==0{return plans,nil};marks=strings.TrimSuffix(strings.Repeat("?,",len(eligible)),",");rows,e=db.QueryContext(ctx,`SELECT id,payload_json FROM conversation_provider_events WHERE id IN (`+marks+`)`,eligible...);if e!=nil{return nil,e};byID:=map[int64]int{};for i,p:=range plans{byID[p.rawCache.ID]=i}
 for rows.Next(){var id int64;var body string;if e=rows.Scan(&id,&body);e!=nil{break};i:=byID[id];if plans[i].skip!=""{continue};p:=plans[i].rawCache;p.Old=body;p.Small,e=rawCacheSmall(ctx,body);if e!=nil{plans[i].skip="raw_cache_invalid_or_unsupported_json";e=nil;continue};if len(p.Small)>=len(body)&&!p.Audit{plans[i].skip="raw_cache_already_small"};if p.Small==body&&!p.Audit{plans[i].skip="raw_cache_already_compact"}}
 if e==nil{e=rows.Err()};rows.Close();return plans,e
}
func applyRawCache(ctx context.Context,tx *sql.Tx,p maintenancePrepared,cutoff time.Time,result *HistoryMaintenanceResult)(bool,error){
 if p.skip!=""{result.Retained[p.skip]++;return true,nil};if !result.rawCacheSchemaChecked{if e:=rawCacheSchema(ctx,tx);e!=nil{return false,e};result.rawCacheSchemaChecked=true}
 old:=p.rawCache;current:=&rawCachePlan{};var payload string
 e:=tx.QueryRowContext(ctx,`SELECT `+rawCacheMetadata+`,payload_json FROM conversation_provider_events WHERE id=?`,old.ID).Scan(&current.ID,&current.Conversation,&current.Session,&current.Event,&current.Method,&current.Received,&current.Branch,&current.Review,&payload)
 if errors.Is(e,sql.ErrNoRows){result.Retained["row_missing"]++;return true,nil};if e!=nil{return false,e}
 if !reflect.DeepEqual(current.values(""),old.values(""))||payload!=old.Old{result.Retained["concurrent_body_or_owner_change"]++;return false,nil}
 if reason:=historyBodyCoverageReason(ctx,maintenanceRow{"id":old.ID},"raw");reason!=""{result.Retained[reason]++;return true,nil}
 reason,e:=historyGCEligible(ctx,tx,historyGCRow{conversation:old.Conversation,received:old.Received},HistoryGCOptions{Cutoff:cutoff},map[string]historyGCProtection{});if e!=nil{return false,e};if reason!=""{result.Retained[reason]++;return true,nil}
 if old.Small!=old.Old{
  var seqBefore,seqAfter int64;if e=tx.QueryRowContext(ctx,`SELECT seq FROM sqlite_sequence WHERE name='conversation_provider_events'`).Scan(&seqBefore);e!=nil{return false,e}
  removed,x:=tx.ExecContext(ctx,`DELETE FROM conversation_provider_events WHERE id=? AND payload_json IS ?`,old.ID,old.Old);if x!=nil{return false,x};n,x:=removed.RowsAffected();if x!=nil||n!=1{if x!=nil{return false,x};return false,errors.New("raw_cache_CAS_changed")}
  if _,e=tx.ExecContext(ctx,rawCacheInsert,old.values(old.Small)...);e!=nil{return false,e}
  if e=tx.QueryRowContext(ctx,`SELECT seq FROM sqlite_sequence WHERE name='conversation_provider_events'`).Scan(&seqAfter);e!=nil{return false,e};if seqAfter!=seqBefore{return false,errors.New("raw_cache_sequence_changed")}
  result.Compacted++;result.LogicalBytes+=int64(len(old.Old)-len(old.Small))
 }
 deleted,e:=tx.ExecContext(ctx,`DELETE FROM ao_history_retention WHERE object_kind='provider_event' AND object_id=?`,maintenanceID(old.ID));if e!=nil{return false,e};n,e:=deleted.RowsAffected();if e!=nil{return false,e};result.AuditDeleted+=int(n);return true,nil
}
