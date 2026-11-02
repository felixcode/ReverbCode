package store
import("context";"database/sql";"path/filepath";"testing";"time")
func TestSecondaryActivityFastKeepDoesNotReadTailOrPromoteChangedRows(t *testing.T){
 ctx:=context.Background();db,e:=sql.Open("sqlite",filepath.Join(t.TempDir(),"tiny.db"));if e!=nil{t.Fatal(e)};defer db.Close()
 // Deliberately omit metadata tail columns. Retained native rows must never
 // issue the full metadata query, regardless of output size in real schemas.
 if _,e=db.Exec(`CREATE TABLE conversation_activities(id TEXT PRIMARY KEY,provider_item_id);INSERT INTO conversation_activities VALUES('native','native-id'),('invalid',1)`);e!=nil{t.Fatal(e)}
 plans,e:=prepareHistoryGCActivities(ctx,db,[]any{"native","invalid"});if e!=nil||len(plans)!=2||plans[0].skip!="activity_native_or_request_identity"||plans[1].skip!="activity_identity_type_unknown"{t.Fatalf("identity-only plans %+v %v",plans,e)}
 if _,e=db.Exec(`UPDATE conversation_activities SET provider_item_id='' WHERE id='native'`);e!=nil{t.Fatal(e)}
 tx,e:=db.BeginTx(ctx,nil);if e!=nil{t.Fatal(e)};defer tx.Rollback();r:=HistoryMaintenanceResult{Retained:map[string]int{}}
 for _,p:=range plans{advanced,e:=applyHistoryGCActivity(ctx,tx,p,time.Now().Add(-48*time.Hour),&r);if e!=nil||!advanced{t.Fatalf("keep changed to DML %+v %v",r,e)}}
 if r.Deleted!=0||r.AuditDeleted!=0||r.Retained["activity_native_or_request_identity"]!=1||r.Retained["activity_identity_type_unknown"]!=1{t.Fatalf("retained plans mutated %+v",r)}
 var n int;if e=tx.QueryRow(`SELECT count(*) FROM conversation_activities`).Scan(&n);e!=nil||n!=2{t.Fatal("retained row deleted")}
}
