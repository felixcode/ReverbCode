package store
import("context";"testing")
func TestHistoryGCOfflineBulkVacuumDrainsAndRollsBack(t *testing.T){
 for _,finish:=range []string{"commit","rollback","cancel"}{t.Run(finish,func(t *testing.T){
  w,_,_:=reclaimFixture(t);before:=reclaimState(t,w);ctx,cancel:=context.WithCancel(context.Background());defer cancel();tx,e:=w.BeginTx(ctx,nil);if e!=nil{t.Fatal(e)};defer tx.Rollback()
  rows,e:=historyGCDrainVacuum(ctx,tx,16);if e!=nil{t.Fatal(e)};inside:=reclaimState(t,tx)
  if before.Free-inside.Free!=16||before.Pages-inside.Pages<16{t.Fatalf("bulk drain failed before=%+v inside=%+v rows=%d",before,inside,rows)}
  if finish=="commit"{if e=tx.Commit();e!=nil{t.Fatal(e)}}else{if finish=="cancel"{cancel()};tx.Rollback()}
  after:=reclaimState(t,w);if finish=="commit"{if after!=inside{t.Fatal("bulk commit lost",before,inside,after)}}else if after!=before{t.Fatal("bulk rollback/cancel changed pages",before,after)}
 })}
}
func TestHistoryGCOfflineBulkReclaimReportsConfirmedDelta(t *testing.T){
 w,r,_:=reclaimFixture(t);s:=NewStore(w,r);before:=reclaimState(t,w);v,e:=s.ReclaimHistoryGCOffline(context.Background(),16);if e!=nil||!v.Committed||v.CommitState!="committed"||v.FreelistPagesRemoved!=16||v.LogicalPagesRemoved<16{t.Fatalf("offline result %+v %v",v,e)}
 var n int;w.QueryRow(`SELECT count(*) FROM payloads`).Scan(&n);if n!=1{t.Fatal("live payload removed")};if after:=reclaimState(t,w);before.Free-after.Free!=16{t.Fatal("wrong physical delta",before,after)}
 ctx,cancel:=context.WithCancel(context.Background());cancel();v,e=s.ReclaimHistoryGCOffline(ctx,16);if e==nil||v.Committed||v.FreelistPagesRemoved!=0{t.Fatalf("cancel reported commit %+v %v",v,e)}
}
