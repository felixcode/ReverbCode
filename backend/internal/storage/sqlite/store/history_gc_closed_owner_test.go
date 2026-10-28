package store_test

import (
 "context"
 "database/sql"
 "path/filepath"
 "testing"
 "time"
 "github.com/aoagents/agent-orchestrator/backend/internal/domain"
 "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
 adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

type closedOwnerFixture struct{s *sqlite.Store;db *sql.DB;owner,conv,branch string;now time.Time}
func closedOwnerSeed(t *testing.T)closedOwnerFixture{
 t.Helper();ctx:=context.Background();dir:=t.TempDir();s,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};t.Cleanup(func(){s.Close()})
 seedProject(t,s,"closed-owner");rec:=sampleRecord("closed-owner");rec.Mode=domain.SessionModeChat;owner,e:=s.CreateSession(ctx,rec);if e!=nil{t.Fatal(e)}
 conv,e:=s.CreateConversation(ctx,"closed-conv",domain.ConversationScopeSession,"closed-owner",owner.ID,histClock);if e!=nil{t.Fatal(e)}
 if e=s.ClaimChatControllerGeneration(ctx,owner.ID,"gen");e!=nil{t.Fatal(e)}
 if _,e=s.AppendUserMessage(ctx,conv.ID,owner.ID,"gen",domain.ConversationMessage{ID:"original-input",Text:"PRIVATE ORIGINAL",Origin:domain.MessageOriginHuman,ClientMessageID:"client-anchor"},"terminal-turn",histClock);e!=nil{t.Fatal(e)}
 if e=s.BindTurnToProvider(ctx,"terminal-turn","native-turn",histClock);e!=nil{t.Fatal(e)}
 if e=s.SettleTurnByID(ctx,"terminal-turn",domain.TurnStateCompleted,"",histClock);e!=nil{t.Fatal(e)}
 db,e:=sql.Open("sqlite",filepath.Join(dir,"ao.db"));if e!=nil{t.Fatal(e)};t.Cleanup(func(){db.Close()})
 var branch string;if e=db.QueryRow(`SELECT active_branch_id FROM conversations WHERE id=?`,conv.ID).Scan(&branch);e!=nil{t.Fatal(e)}
 if _,e=db.Exec(`UPDATE conversation_branches SET provider_conversation_id='native-thread' WHERE id=?`,branch);e!=nil{t.Fatal(e)}
 if _,e=db.Exec(`UPDATE sessions SET is_terminated=1 WHERE id=?`,owner.ID);e!=nil{t.Fatal(e)}
 f:=closedOwnerFixture{s,db,string(owner.ID),string(conv.ID),branch,time.Now().UTC()};old:=f.now.Add(-72*time.Hour).Format(time.RFC3339Nano)
 for _,item:=range []struct{id int;method,payload string}{{1,"message.delta",`{"text":"old"}`},{2,"turn.completed",`{"providerTurnId":"native-turn","turnState":"completed"}`},{3,"turn.completed",`{"providerTurnId":"native-turn","turnState":"completed","providerConversationId":"native-thread"}`},{4,"queue.preserved_recovered",`{"proof":true}`},{5,"message.delta",`{}`},{6,"turn.completed",`{"providerTurnId":123,"turnState":"completed"}`},{10,"message.delta",`{}`}}{
  stamp:=old;if item.id==5{stamp=f.now.Format(time.RFC3339Nano)}
  if _,e=db.Exec(`INSERT INTO conversation_provider_events(id,conversation_id,session_id,provider_event_id,method,payload_json,received_at,branch_id)VALUES(?,?,?,?,?,?,?,?)`,item.id,conv.ID,owner.ID,"native-key-"+time.Duration(item.id).String(),item.method,item.payload,stamp,branch);e!=nil{t.Fatal(e)}
 }
 if _,e=db.Exec(`INSERT INTO ao_history_retention VALUES('provider_event','1',?,'','',0,'',?,?,'[]')`,conv.ID,f.now.Format(time.RFC3339Nano),old);e!=nil{t.Fatal(e)}
 return f
}
func(f closedOwnerFixture) request(stage string)adapter.HistoryMaintenanceRequest{
 cap:=f.s.HistoryRetentionCapability();return adapter.HistoryMaintenanceRequest{Stage:stage,Cutoff:f.now.Add(-48*time.Hour).Format(time.RFC3339Nano),DatabaseDevice:cap["dbDevice"].(uint64),DatabaseInode:cap["dbInode"].(uint64),OfflineCoverageBefore:f.now.Add(-time.Hour),OfflineMaxEventID:6}
}
func TestClosedOwnerGCDeletesNativeOldRowsPreservingAuthQueueAndInputs(t *testing.T){
 f:=closedOwnerSeed(t);o:=adapter.DefaultHistoryMaintenanceOptions();o.ReadBudget=5*time.Second;o.WriteBudget=5*time.Second
 r,e:=f.s.MaintainHistorySnapshotBatch(context.Background(),f.request("gc:closed-raw"),o,f.now);if e!=nil||!r.Committed||r.Deleted!=3||r.AuditDeleted!=1{t.Fatalf("closed raw %+v %v",r,e)}
 for _,id:=range []int{3,4,5,10}{var n int;if e=f.db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE id=?`,id).Scan(&n);e!=nil||n!=1{t.Fatalf("anchor/new row %d not preserved",id)}}
 var n int;f.db.QueryRow(`SELECT count(*) FROM conversation_messages WHERE id='original-input' AND client_message_id='client-anchor'`).Scan(&n);if n!=1{t.Fatal("original input changed")};f.db.QueryRow(`SELECT count(*) FROM conversation_turns WHERE id='terminal-turn'`).Scan(&n);if n!=1{t.Fatal("turn removed")}
}
func TestClosedOwnerGCRequiresOfflineProofAndFreshClosedCheckpoint(t *testing.T){
 f:=closedOwnerSeed(t);o:=adapter.DefaultHistoryMaintenanceOptions();o.ReadBudget=5*time.Second;o.WriteBudget=5*time.Second
 req:=f.request("gc:closed-raw");req.OfflineCoverageBefore=time.Time{};req.OfflineMaxEventID=0
 r,e:=f.s.MaintainHistorySnapshotBatch(context.Background(),req,o,f.now);if e!=nil||r.Deleted!=0{t.Fatalf("wire context enabled offline deletion %+v %v",r,e)}
 for _,q:=range []string{`UPDATE sessions SET is_terminated=0`,`UPDATE sessions SET is_terminated=1,conversation_checkpoint_unsettled=1`}{if _,e=f.db.Exec(q);e!=nil{t.Fatal(e)};r,e=f.s.MaintainHistorySnapshotBatch(context.Background(),f.request("gc:closed-raw"),o,f.now);if e!=nil||r.Deleted!=0{t.Fatalf("current/checkpoint deleted %+v %v",r,e)}}
}
func TestClosedOwnerGCAuditAndSourceRollbackIsAtomic(t *testing.T){
 f:=closedOwnerSeed(t);if _,e:=f.db.Exec(`CREATE TRIGGER reject_closed BEFORE DELETE ON conversation_provider_events BEGIN SELECT RAISE(ABORT,'synthetic');END`);e!=nil{t.Fatal(e)}
 o:=adapter.DefaultHistoryMaintenanceOptions();o.ReadBudget=5*time.Second;o.WriteBudget=5*time.Second
 r,e:=f.s.MaintainHistorySnapshotBatch(context.Background(),f.request("gc:closed-raw"),o,f.now);if e==nil||r.Deleted!=0||r.AuditDeleted!=0||r.NextCursor!=""{t.Fatalf("failed prefix advanced %+v %v",r,e)}
 var n int;f.db.QueryRow(`SELECT count(*) FROM ao_history_retention WHERE object_kind='provider_event' AND object_id='1'`).Scan(&n);if n!=1{t.Fatal("audit not restored")};f.db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE id=1`).Scan(&n);if n!=1{t.Fatal("row not restored")}
}
func TestClosedOwnerGCActivityKeepsSequenceWatermarkAndRecentMutableRows(t *testing.T){
 f:=closedOwnerSeed(t);old:=f.now.Add(-72*time.Hour).Format(time.RFC3339Nano)
 for i,id:=range []string{"old-native","old-anon","recent-mutable","last-watermark"}{stamp:=old;if id=="recent-mutable"{stamp=f.now.Format(time.RFC3339Nano)};if _,e:=f.db.Exec(`INSERT INTO conversation_activities(id,conversation_id,turn_id,sequence,kind,status,provider_item_id,created_at,updated_at,branch_id)VALUES(?,?,?,?,'command','completed',?,?,?,?)`,id,f.conv,"terminal-turn",i+1,"native-"+id,old,stamp,f.branch);e!=nil{t.Fatal(e)}}
 for _,kind:=range []string{"activity","activity_output"}{if _,e:=f.db.Exec(`INSERT INTO ao_history_retention VALUES(?, 'old-native',?,'','',0,'',?,?,'[]')`,kind,f.conv,f.now.Format(time.RFC3339Nano),old);e!=nil{t.Fatal(e)}}
 o:=adapter.DefaultHistoryMaintenanceOptions();o.ReadBudget=5*time.Second;o.WriteBudget=5*time.Second
 r,e:=f.s.MaintainHistorySnapshotBatch(context.Background(),f.request("gc:closed-activity"),o,f.now);if e!=nil||r.Deleted!=2||r.AuditDeleted!=2{t.Fatalf("closed activities %+v %v",r,e)}
 for _,id:=range []string{"recent-mutable","last-watermark"}{var n int;f.db.QueryRow(`SELECT count(*) FROM conversation_activities WHERE id=?`,id).Scan(&n);if n!=1{t.Fatal("mutable/watermark lost")}}
}
