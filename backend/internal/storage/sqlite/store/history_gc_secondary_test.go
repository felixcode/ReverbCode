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

func TestSecondaryActivityGCDeletesOnlyProvenRowsAndBlocksLateReplay(t *testing.T){
 ctx:=context.Background();dir:=t.TempDir();s,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};defer s.Close()
 seedProject(t,s,"secondary");rec:=sampleRecord("secondary");rec.Mode=domain.SessionModeChat;owner,e:=s.CreateSession(ctx,rec);if e!=nil{t.Fatal(e)}
 conv,e:=s.CreateConversation(ctx,"secondary-conv",domain.ConversationScopeSession,"secondary",owner.ID,histClock);if e!=nil{t.Fatal(e)}
 if e=s.ClaimChatControllerGeneration(ctx,owner.ID,"gen");e!=nil{t.Fatal(e)}
 if _,e=s.AppendUserMessage(ctx,conv.ID,owner.ID,"gen",domain.ConversationMessage{ID:"msg",Text:"synthetic",Origin:domain.MessageOriginHuman,ClientMessageID:"client"},"turn",histClock);e!=nil{t.Fatal(e)}
 if e=s.BindTurnToProvider(ctx,"turn","native-turn",histClock);e!=nil{t.Fatal(e)}
 if e=s.SettleTurnByID(ctx,"turn",domain.TurnStateCompleted,"",histClock);e!=nil{t.Fatal(e)}
 for _,a:=range []domain.ConversationActivity{
  {ID:"delete",Kind:domain.ActivityKindCommand,Status:domain.ActivityStatusCompleted,Summary:"PRIVATE-OLD"},
  {ID:"keep-system",Kind:domain.ActivityKindSystem,Status:domain.ActivityStatusCompleted,Summary:"COMPACTION-ANCHOR"},
  {ID:"keep-native",ProviderItemID:"native-item",Kind:domain.ActivityKindCommand,Status:domain.ActivityStatusCompleted},
  {ID:"keep-pending",RequestID:"approval",Kind:domain.ActivityKindApproval,Status:domain.ActivityStatusPending},
 }{if e=s.UpsertActivity(ctx,conv.ID,"native-turn",a,histClock);e!=nil{t.Fatal(e)}}
 db,e:=sql.Open("sqlite",filepath.Join(dir,"ao.db"));if e!=nil{t.Fatal(e)};defer db.Close()
 old:=time.Now().UTC().Add(-72*time.Hour).Format(time.RFC3339Nano)
 for _,pair:=range [][2]string{{"turn","turn"},{"activity","delete"},{"activity_output","delete"}} {
  if _,e=db.Exec(`INSERT INTO ao_history_retention VALUES(?,?,?,'','',0,'',?,?,'[]')`,pair[0],pair[1],conv.ID,old,old);e!=nil{t.Fatal(e)}
 }
 cap:=s.HistoryRetentionCapability();o:=adapter.DefaultHistoryMaintenanceOptions();req:=adapter.HistoryMaintenanceRequest{Stage:"gc:activity",Cutoff:time.Now().UTC().Add(-48*time.Hour).Format(time.RFC3339Nano),DatabaseDevice:cap["dbDevice"].(uint64),DatabaseInode:cap["dbInode"].(uint64)}
 // Pending user input prevents deletion even though the turn is terminal/marked.
 got,e:=s.MaintainHistoryBatch(ctx,req,o);if e!=nil||got.Deleted!=0{t.Fatalf("pending not protected %+v %v",got,e)}
 if _,e=db.Exec(`UPDATE conversation_activities SET status='resolved' WHERE id='keep-pending'`);e!=nil{t.Fatal(e)}
 if _,e=db.Exec(`CREATE TRIGGER reject_secondary_delete BEFORE DELETE ON conversation_activities BEGIN SELECT RAISE(ABORT,'synthetic');END`);e!=nil{t.Fatal(e)}
 got,e=s.MaintainHistoryBatch(ctx,req,o);if e==nil||got.Deleted!=0||got.AuditDeleted!=0{t.Fatalf("failed delete reported success %+v %v",got,e)}
 var auditCount int;db.QueryRow(`SELECT count(*) FROM ao_history_retention WHERE object_id='delete'`).Scan(&auditCount);if auditCount!=2{t.Fatal("source/audit rollback was not atomic")}
 if _,e=db.Exec(`DROP TRIGGER reject_secondary_delete`);e!=nil{t.Fatal(e)}
 got,e=s.MaintainHistoryBatch(ctx,req,o);if e!=nil||!got.Committed||got.Deleted!=1||got.AuditDeleted!=2||got.LogicalBytes<=0{t.Fatalf("proven GC %+v %v",got,e)}
 var n int;db.QueryRow(`SELECT count(*) FROM ao_history_retention WHERE object_kind='turn' AND object_id='turn'`).Scan(&n);if n!=1{t.Fatal("turn replay marker removed")}
 db.QueryRow(`SELECT count(*) FROM conversation_activities WHERE id IN ('keep-system','keep-native','keep-pending')`).Scan(&n);if n!=3{t.Fatal("anchor/native/answer row removed")}
 if e=s.UpsertActivity(ctx,conv.ID,"native-turn",domain.ConversationActivity{ID:"late-new-id",Kind:domain.ActivityKindCommand,Status:domain.ActivityStatusCompleted,Summary:"MUST-NOT-RETURN"},time.Now());e!=nil{t.Fatal(e)}
 db.QueryRow(`SELECT count(*) FROM conversation_activities WHERE id='late-new-id'`).Scan(&n);if n!=0{t.Fatal("late expired anonymous activity resurrected")}
 // A current running turn with a different native identity remains writable.
 if _,e=s.AppendUserMessage(ctx,conv.ID,owner.ID,"gen",domain.ConversationMessage{ID:"new-msg",Text:"new",Origin:domain.MessageOriginHuman,ClientMessageID:"new-client"},"new-turn",time.Now());e!=nil{t.Fatal(e)}
 if e=s.BindTurnToProvider(ctx,"new-turn","new-native",time.Now());e!=nil{t.Fatal(e)}
 if e=s.UpsertActivity(ctx,conv.ID,"new-native",domain.ConversationActivity{ID:"new-activity",Kind:domain.ActivityKindCommand,Status:domain.ActivityStatusCompleted},time.Now());e!=nil{t.Fatal(e)}
 db.QueryRow(`SELECT count(*) FROM conversation_activities WHERE id='new-activity'`).Scan(&n);if n!=1{t.Fatal("new turn suppressed")}
}

func TestSecondaryAuditGCOnlyOldKnownSourceMissing(t *testing.T){
 ctx:=context.Background();dir:=t.TempDir();s,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};defer s.Close()
 db,e:=sql.Open("sqlite",filepath.Join(dir,"ao.db"));if e!=nil{t.Fatal(e)};defer db.Close()
 seedProject(t,s,"audit-present");rec:=sampleRecord("audit-present");owner,e:=s.CreateSession(ctx,rec);if e!=nil{t.Fatal(e)}
 old:=time.Now().UTC().Add(-72*time.Hour).Format(time.RFC3339Nano);fresh:=time.Now().UTC().Format(time.RFC3339Nano)
 for _,a:=range []struct{kind,key,at string}{{"activity","missing",old},{"message_coordination","missing-message",old},{"provider_event","missing-event",old},{"unknown","x",old},{"turn","recent",fresh},{"turn","bad-time","bad"}} {
  if _,e=db.Exec(`INSERT INTO ao_history_retention VALUES(?,?,'c','','',0,'',?,?,'[]')`,a.kind,a.key,a.at,old);e!=nil{t.Fatal(e)}
 }
 if _,e=db.Exec(`INSERT INTO ao_history_retention VALUES('session',?,'c','','',0,'',?,?,'[]')`,owner.ID,old,old);e!=nil{t.Fatal(e)}
 cap:=s.HistoryRetentionCapability();o:=adapter.DefaultHistoryMaintenanceOptions();req:=adapter.HistoryMaintenanceRequest{Stage:"gc:audit",Cutoff:time.Now().UTC().Add(-48*time.Hour).Format(time.RFC3339Nano),DatabaseDevice:cap["dbDevice"].(uint64),DatabaseInode:cap["dbInode"].(uint64)}
 got,e:=s.MaintainHistoryBatch(ctx,req,o);if e!=nil||got.AuditDeleted!=2||got.Deleted!=2||got.Retained["audit_provider_event_owned"]!=1||got.Retained["audit_kind_unknown"]!=1||got.Retained["audit_within_48h"]!=1||got.Retained["audit_invalid_timestamp"]!=1||got.Retained["audit_source_present"]!=1{t.Fatalf("audit policy %+v %v",got,e)}
}
