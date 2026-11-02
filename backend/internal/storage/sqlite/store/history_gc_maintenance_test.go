package store
import("context";"testing";"time")
func TestHistoryGCRoutineRechecksPendingAfterRead(t *testing.T){
 _,db,now:=gcFixture(t);ctx:=context.Background();gcInsert(t,db,1,"c","","message.delta",now.Add(-72*time.Hour).Format(time.RFC3339Nano))
 plans,e:=prepareHistoryGC(ctx,db,[]any{int64(1)});if e!=nil{t.Fatal(e)}
 if _,e=db.Exec(`INSERT INTO conversation_turns VALUES('new','c','running',?,NULL)`,now.Add(-96*time.Hour).Format(time.RFC3339Nano));e!=nil{t.Fatal(e)}
 tx,e:=db.BeginTx(ctx,nil);if e!=nil{t.Fatal(e)};defer tx.Rollback();r:=HistoryMaintenanceResult{Retained:map[string]int{}}
 advanced,e:=applyHistoryGC(ctx,tx,plans[0],now.Add(-48*time.Hour),&r);if e!=nil||!advanced||r.Deleted!=0||r.Retained["unresolved_tail"]!=1{t.Fatalf("pending recheck %+v %v",r,e)}
}
func TestHistoryGCRoutineAuditAndEventRollbackTogether(t *testing.T){
 _,db,now:=gcFixture(t);ctx:=context.Background();gcInsert(t,db,1,"c","","message.delta",now.Add(-72*time.Hour).Format(time.RFC3339Nano));db.Exec(`INSERT INTO ao_history_retention VALUES('provider_event','1','2020-01-01 00:00:00');CREATE TRIGGER reject_daily BEFORE DELETE ON conversation_provider_events BEGIN SELECT RAISE(ABORT,'synthetic');END`)
 plans,e:=prepareHistoryGC(ctx,db,[]any{int64(1)});if e!=nil{t.Fatal(e)};tx,e:=db.BeginTx(ctx,nil);if e!=nil{t.Fatal(e)};r:=HistoryMaintenanceResult{Retained:map[string]int{}}
 if _,e=applyHistoryGC(ctx,tx,plans[0],now.Add(-48*time.Hour),&r);e==nil{t.Fatal("delete error hidden")};tx.Rollback();var n int;db.QueryRow(`SELECT count(*) FROM ao_history_retention`).Scan(&n);if n!=1{t.Fatal("audit was not rolled back")}
}
