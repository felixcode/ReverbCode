package store
import("context";"testing";"time")
func TestOfflineBodyRawFallbackOnlyProjectedOldMethods(t *testing.T){
 _,db,now:=gcFixture(t);if _,e:=db.Exec(`CREATE TABLE sessions(id TEXT,is_terminated INTEGER);INSERT INTO sessions VALUES('active',0)`);e!=nil{t.Fatal(e)}
 row:=maintenanceRow{"session_id":"active","conversation_id":"c","received_at":now.Add(-72*time.Hour).Format(time.RFC3339Nano),"method":"message.delta","provider_event_id":"KEEP-DEDUPE"};proof:=&maintenanceRawProof{hasBody:true}
 ok,e:=maintenanceEligibleWithProof(context.Background(),db,row,"raw",now.Add(-48*time.Hour),proof);if e!=nil||!ok{t.Fatal("projected fallback",ok,e)}
 row["method"]="turn.completed";if ok,e=maintenanceEligibleWithProof(context.Background(),db,row,"raw",now.Add(-48*time.Hour),proof);e==nil||ok{t.Fatal("control unexpectedly eligible",ok,e)}
 row["method"]="message.delta";row["received_at"]=now.Add(-24*time.Hour).Format(time.RFC3339Nano);if ok,_=maintenanceEligibleWithProof(context.Background(),db,row,"raw",now.Add(-12*time.Hour),proof);ok{t.Fatal("fallback younger than48h")}
 row["received_at"]=now.Add(-72*time.Hour).Format(time.RFC3339Nano);db.Exec(`INSERT INTO conversation_turns VALUES('pending','c','running',?,NULL)`,now.Add(-96*time.Hour).Format(time.RFC3339Nano));if ok,_=maintenanceEligibleWithProof(context.Background(),db,row,"raw",now.Add(-48*time.Hour),proof);ok{t.Fatal("pending tail expired")}
}

func TestOfflineBodyCoverageAndExactMarkers(t *testing.T){
 now:=time.Now().UTC();ctx:=context.WithValue(context.Background(),historyBodyCoverageKey{},historyBodyCoverage{Before:now.Add(-time.Hour),MaxEventID:50})
 if historyBodyCoverageReason(ctx,maintenanceRow{"id":int64(51)},"raw")!="offline_event_after_backup"{t.Fatal("new event not protected")}
 if historyBodyCoverageReason(ctx,maintenanceRow{"updated_at":now.Format(time.RFC3339Nano)},"activity")!="offline_updated_after_backup"{t.Fatal("mutable new body not protected")}
 if historyBodyCoverageReason(ctx,maintenanceRow{},"turn")!="offline_turn_mutable_coverage_unknown"{t.Fatal("unversioned turn not protected")}
 if !historyBodyAlreadyEmpty(maintenanceRow{"summary":maintenanceExpired,"detail_json":`{"_ao_history_expired":true}`},"activity"){t.Fatal("exact expired marker not empty")}
 if historyBodyAlreadyEmpty(maintenanceRow{"summary":"KEEP-REAL"},"activity"){t.Fatal("real body treated as empty")}
 if !historyBodyValueMarker(map[string]any{"_ao_history_expired":true})||historyBodyValueMarker(map[string]any{"_ao_history_expired":true,"text":"KEEP"}){t.Fatal("marker whitelist too broad")}
}
