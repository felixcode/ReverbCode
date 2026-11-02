package store_test
import("context";"database/sql";"path/filepath";"testing";"time";"github.com/aoagents/agent-orchestrator/backend/internal/domain";"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite";adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store")
func TestHistoryGCOwnedMaintenanceStage(t *testing.T){
 ctx:=context.Background();dir:=t.TempDir();s,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};defer s.Close();seedProject(t,s,"gc-owned");rec:=sampleRecord("gc-owned");rec.Mode=domain.SessionModeChat;session,e:=s.CreateSession(ctx,rec);if e!=nil{t.Fatal(e)};conv,e:=s.CreateConversation(ctx,"gc-owned-conv",domain.ConversationScopeSession,"gc-owned",session.ID,histClock);if e!=nil{t.Fatal(e)}
 db,e:=sql.Open("sqlite",filepath.Join(dir,"ao.db"));if e!=nil{t.Fatal(e)};defer db.Close();for _,v:=range []struct{key,method string}{{"","message.delta"},{"native","message.delta"},{"","turn.completed"}}{if _,e=db.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,?,?,?,?)`,conv.ID,session.ID,v.key,v.method,`{}`,histClock);e!=nil{t.Fatal(e)}}
 cap:=s.HistoryRetentionCapability();o:=adapter.DefaultHistoryMaintenanceOptions();o.BatchRows=25;r:=adapter.HistoryMaintenanceRequest{Stage:"gc:raw",Cutoff:time.Now().UTC().Add(-48*time.Hour).Format(time.RFC3339Nano),DatabaseDevice:cap["dbDevice"].(uint64),DatabaseInode:cap["dbInode"].(uint64)}
 got,e:=s.MaintainHistoryBatch(ctx,r,o);if e!=nil||!got.Committed||got.Deleted!=1||got.Scanned!=3||got.Markers!=0{t.Fatalf("gc stage %+v %v",got,e)}
 r.Cutoff=time.Now().UTC().Add(-12*time.Hour).Format(time.RFC3339Nano);if _,e=s.MaintainHistoryBatch(ctx,r,o);e==nil{t.Fatal("12h GC cutoff accepted")}
}
