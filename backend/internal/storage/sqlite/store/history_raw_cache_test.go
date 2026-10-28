package store_test
import("context";"crypto/sha256";"database/sql";"encoding/json";"fmt";"path/filepath";"strings";"testing";"time";"github.com/aoagents/agent-orchestrator/backend/internal/domain";"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite";adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store")
func cacheFixture(t *testing.T)(*sqlite.Store,*sql.DB,string,string){
 t.Helper();dir:=t.TempDir();s,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};t.Cleanup(func(){s.Close()});seedProject(t,s,"raw-cache");r:=sampleRecord("raw-cache");r.Mode=domain.SessionModeChat;session,e:=s.CreateSession(context.Background(),r);if e!=nil{t.Fatal(e)};conv,e:=s.CreateConversation(context.Background(),"raw-cache-conv",domain.ConversationScopeSession,"raw-cache",session.ID,histClock);if e!=nil{t.Fatal(e)};db,e:=sql.Open("sqlite",filepath.Join(dir,"ao.db"));if e!=nil{t.Fatal(e)};t.Cleanup(func(){db.Close()});return s,db,conv.ID,string(session.ID)
}
func cacheRun(t *testing.T,s *sqlite.Store,cursor string,maxID int64)(adapter.HistoryMaintenanceResult,error){
 t.Helper();cap:=s.HistoryRetentionCapability();now:=time.Now().UTC();o:=adapter.DefaultHistoryMaintenanceOptions();o.RawCacheRetention=6*time.Hour;o.BatchRows=250;o.RawCacheBatchRows=250;o.ReadBudget=2*time.Second;o.WriteBudget=2*time.Second
 return s.MaintainHistorySnapshotBatch(context.Background(),adapter.HistoryMaintenanceRequest{Stage:"cache:raw",Cursor:cursor,Cutoff:now.Add(-6*time.Hour).Format(time.RFC3339Nano),DatabaseDevice:cap["dbDevice"].(uint64),DatabaseInode:cap["dbInode"].(uint64),OfflineCoverageBefore:now.Add(-time.Hour),OfflineMaxEventID:maxID},o,now)
}
func cacheMeta(t *testing.T,db *sql.DB)[32]byte{
 t.Helper();rows,e:=db.Query(`SELECT rowid,id,conversation_id,session_id,provider_event_id,method,CAST(received_at AS TEXT),branch_id,review_id FROM conversation_provider_events ORDER BY id`);if e!=nil{t.Fatal(e)};defer rows.Close();var all [][]any
 for rows.Next(){values:=make([]any,9);ptr:=make([]any,9);for i:=range values{ptr[i]=&values[i]};if e=rows.Scan(ptr...);e!=nil{t.Fatal(e)};all=append(all,values)};if e=rows.Err();e!=nil{t.Fatal(e)};b,_:=json.Marshal(all);return sha256.Sum256(b)
}
func TestRawCacheCompactsFull250AndKeepsMetadataSequence(t *testing.T){
 s,db,conv,session:=cacheFixture(t);old:=time.Now().UTC().Add(-8*time.Hour).Format(time.RFC3339Nano);body:=`{"providerTurnId":"native-turn","providerConversationId":"native-conv","detail":{"copy":"`+strings.Repeat("X",800)+`"},"_retention":{"protocol":"foreign-debug"}}`
 tx,e:=db.Begin();if e!=nil{t.Fatal(e)};for i:=1;i<=250;i++{if _,e=tx.Exec(`INSERT INTO conversation_provider_events(id,conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,?,?,?,?,?)`,i,conv,session,fmt.Sprint("native-",i),"message.completed",body,old);e!=nil{t.Fatal(e)};if _,e=tx.Exec(`INSERT INTO ao_history_retention VALUES('provider_event',?,?,'semantic','prompt',1000,'original',?,?, '[]')`,fmt.Sprint(i),conv,old,old);e!=nil{t.Fatal(e)}};if e=tx.Commit();e!=nil{t.Fatal(e)}
 before:=cacheMeta(t,db);var seq int;db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='conversation_provider_events'`).Scan(&seq)
 r,e:=cacheRun(t,s,"",250);if e!=nil||r.Compacted!=250||r.AuditDeleted!=250||r.Markers!=0||!r.Committed{t.Fatalf("cache %+v %v",r,e)};if cacheMeta(t,db)!=before{t.Fatal("identity metadata changed")};var after int;db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='conversation_provider_events'`).Scan(&after);if after!=seq{t.Fatal("sequence changed")};var raw string;db.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE id=1`).Scan(&raw);var v map[string]any;json.Unmarshal([]byte(raw),&v);if len(v)!=3||v["providerTurnId"]!="native-turn"||v["providerConversationId"]!="native-conv"{t.Fatal("wrong skeleton")}
 again,e:=cacheRun(t,s,"",250);if e!=nil||again.Compacted!=0||again.AuditDeleted!=0{t.Fatalf("not idempotent %+v %v",again,e)}
 t.Logf("all_eligible250 held_ms=%f reserved_ms=%f payload_bytes_removed=%d",r.HeldMS,r.WriterReservedMS,r.LogicalBytes)
}
func TestRawCachePreservesRecentControlAndPostBackupRows(t *testing.T){
 s,db,conv,session:=cacheFixture(t);now:=time.Now().UTC();old:=now.Add(-8*time.Hour).Format(time.RFC3339Nano);body:=`{"providerTurnId":"n","text":"KEEP"}`
 for _,r:=range []struct{id int;method,at string}{{1,"turn.completed",old},{2,"message.delta",now.Format(time.RFC3339Nano)},{3,"message.completed",old}}{if _,e:=db.Exec(`INSERT INTO conversation_provider_events(id,conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,?,?,?,?,?)`,r.id,conv,session,fmt.Sprint(r.id),r.method,body,r.at);e!=nil{t.Fatal(e)}}
 r,e:=cacheRun(t,s,"",2);if e!=nil||r.Compacted!=0||r.Retained["raw_cache_method_kept"]!=1||r.Retained["within_raw_cache_window"]!=1||r.Retained["offline_event_after_backup"]!=1{t.Fatalf("guards %+v %v",r,e)};var n int;db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE payload_json=?`,body).Scan(&n);if n!=3{t.Fatal("protected content changed")}
 if e=s.ClaimChatControllerGeneration(context.Background(),domain.SessionID(session),"pending-gen");e!=nil{t.Fatal(e)}
 if _,e=s.AppendUserMessage(context.Background(),conv,domain.SessionID(session),"pending-gen",domain.ConversationMessage{ID:"pending-input",Text:"KEEP-CURRENT",Origin:domain.MessageOriginHuman,ClientMessageID:"pending-client"},"pending-turn",now.Add(-9*time.Hour));e!=nil{t.Fatal(e)}
 if _,e=db.Exec(`INSERT INTO conversation_provider_events(id,conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(4,?,?,'pending-event','message.delta',?,?)`,conv,session,body,old);e!=nil{t.Fatal(e)}
 r,e=cacheRun(t,s,"3",4);if e!=nil||r.Compacted!=0||r.Retained["unresolved_tail"]!=1{t.Fatalf("pending tail %+v %v",r,e)}
}
func TestRawCacheInsertFailureRollsBackCompleteRowAndAudit(t *testing.T){
 s,db,conv,session:=cacheFixture(t);old:=time.Now().UTC().Add(-8*time.Hour).Format(time.RFC3339Nano);body:=`{"providerTurnId":"n","text":"`+strings.Repeat("OLD",100)+`"}`;db.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,'native','message.delta',?,?)`,conv,session,body,old)
 // A CHECK-equivalent dedupe conflict injected after preparation exercises the
 // ordinary INSERT ABORT and complete enclosing maintenance transaction rollback.
 db.Exec(`INSERT INTO ao_history_retention VALUES('provider_event','1',?,'semantic','prompt',1000,'original',?,?,'[]')`,conv,old,old)
 before:=cacheMeta(t,db);db.Exec(`CREATE UNIQUE INDEX synthetic_payload_gate ON conversation_provider_events(length(payload_json))`)
 // The unrelated existing key occupies the compact payload's unique length.
 small:=`{"_rawCache":1,"providerTurnId":"n"}`;db.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,'control','turn.completed',?,?)`,conv,session,strings.Repeat("x",len(small)),old)
 before=cacheMeta(t,db);r,e:=cacheRun(t,s,"",100);if e==nil||r.Compacted!=0||r.Committed||r.NextCursor!=""{t.Fatalf("failed prefix advanced %+v %v",r,e)};if cacheMeta(t,db)!=before{t.Fatal("failed insert lost identities")};var got string;db.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE provider_event_id='native'`).Scan(&got);if got!=body{t.Fatal("failed insert lost original body")};var audits int;db.QueryRow(`SELECT count(*) FROM ao_history_retention WHERE object_kind='provider_event' AND object_id='1'`).Scan(&audits);if audits!=1{t.Fatal("failed insert lost original audit")}
}
