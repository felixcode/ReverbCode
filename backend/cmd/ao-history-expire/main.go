//go:build linux

// An explicitly offline application maintenance entry; never starts AO.
package main

import("context";"encoding/json";"flag";"fmt";"os";"os/signal";"path/filepath";"syscall";"time"
	aosqlite "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func main(){os.Exit(run())}
func run()int{
	var dir,report,lock,cutoffText,coverageText string;var dev,ino uint64;var maxID int64;var rows,checkpointEvery,maxBatches int;var apply bool;var wall,batch,reclaim time.Duration;var pages int64
	flag.StringVar(&dir,"data-dir","","existing stopped AO data directory");flag.StringVar(&report,"report","","private progress JSON")
	flag.StringVar(&lock,"maintenance-lock","","existing shared maintenance lock");flag.StringVar(&cutoffText,"cutoff","","UTC cutoff at least 48h old")
	flag.StringVar(&coverageText,"backup-frozen-at","","existing verified recovery point freeze")
	flag.Uint64Var(&dev,"database-device",0,"expected device");flag.Uint64Var(&ino,"database-inode",0,"expected inode")
	flag.Int64Var(&maxID,"backup-max-event-id",0,"verified pre-restart CPE upper bound")
	flag.IntVar(&rows,"batch-rows",1000,"bounded rows per transaction");flag.BoolVar(&apply,"apply",false,"actually delete; default readonly preview")
	flag.DurationVar(&wall,"wall-budget",2*time.Hour,"total maintenance budget");flag.DurationVar(&batch,"batch-budget",10*time.Second,"one transaction ceiling")
	flag.IntVar(&checkpointEvery,"checkpoint-every-batches",10,"bound WAL growth");flag.DurationVar(&reclaim,"reclaim-budget",10*time.Second,"one incremental reclaim reservation")
	flag.Int64Var(&pages,"reclaim-max-pages",2048,"maximum incremental pages per call");flag.IntVar(&maxBatches,"max-batches",0,"optional bounded run; preview defaults to one batch");flag.Parse()
	cutoff,e:=time.Parse(time.RFC3339Nano,cutoffText);if e!=nil{return fail("invalid_cutoff")};freeze,e:=time.Parse(time.RFC3339Nano,coverageText)
	if e!=nil || dir=="" || report=="" || lock=="" || dev==0 || ino==0 || maxID<=0 || rows<=0 || rows>10000 || checkpointEvery<=0 || wall<=0 || batch<=0 || pages<=0 || reclaim<=0 || cutoff.After(time.Now().UTC().Add(-48*time.Hour)) || freeze.After(time.Now().UTC()){return fail("invalid_options")}
	if !apply && maxBatches==0{maxBatches=1};parent,x:=os.Stat(filepath.Dir(report));if x!=nil || !parent.IsDir() || parent.Mode().Perm()!=0700 || parent.Sys().(*syscall.Stat_t).Uid!=uint32(os.Getuid()){return fail("report_parent_not_private")}
	lf,e:=os.OpenFile(lock,os.O_RDWR,0600);if e!=nil{return fail("maintenance_lock_missing")};defer lf.Close()
	if e=syscall.Flock(int(lf.Fd()),syscall.LOCK_EX|syscall.LOCK_NB);e!=nil{return fail("maintenance_busy")}
	ctx,stop:=signal.NotifyContext(context.Background(),syscall.SIGTERM,syscall.SIGINT);defer stop();ctx,cancel:=context.WithTimeout(ctx,wall);defer cancel()
	s,e:=aosqlite.OpenHistoryGCExisting(ctx,dir,dev,ino,apply);if e!=nil{return fail("open_existing_failed")};defer s.Close()
	state:=map[string]any{"status":"running","stage":"delete_anonymous_stream","apply":apply,"scanned":int64(0),"deleted":int64(0),"audit_deleted":int64(0),"retained":map[string]int{},"confirmed_cursor":int64(0),"backup_max_event_id":maxID,"cutoff":cutoffText,"backup_frozen_at":coverageText}
	initialInfo,_:=os.Stat(filepath.Join(dir,"ao.db"));initialBytes:=initialInfo.Size()
	save:=func()error{if fi,x:=os.Stat(filepath.Join(dir,"ao.db"));x==nil{state["main_file_bytes"]=fi.Size();state["physical_main_bytes_reduced"]=initialBytes-fi.Size()};if fi,x:=os.Stat(filepath.Join(dir,"ao.db-wal"));x==nil{state["wal_file_bytes"]=fi.Size()}else{state["wal_file_bytes"]=int64(0)};var fs syscall.Statfs_t;if syscall.Statfs(dir,&fs)==nil{state["filesystem_available_bytes"]=fs.Bavail*uint64(fs.Bsize)};state["at"]=time.Now().UTC().Format(time.RFC3339Nano);b,x:=json.MarshalIndent(state,"","  ");if x!=nil{return x};tmp:=report+".new";f,x:=os.OpenFile(tmp,os.O_WRONLY|os.O_CREATE|os.O_EXCL,0600);if x!=nil{return x};if _,x=f.Write(b);x==nil{x=f.Sync()};f.Close();if x!=nil{return x};return os.Rename(tmp,report)}
	if e=save();e!=nil{return fail("report_write_failed")}
	var cursor int64;totalScanned,totalDeleted,totalAudits:=int64(0),int64(0),int64(0);totalEligible:=int64(0);retained:=map[string]int{};batches:=0
	for{
		bc,done:=context.WithTimeout(ctx,batch);r,x:=s.PruneAnonymousHistoryBatch(bc,cursor,store.HistoryGCOptions{Cutoff:cutoff,CoverageBefore:freeze,MaxEventID:maxID,BatchRows:rows,Apply:apply});done()
		if x!=nil{state["status"]="failed";state["error"]="gc_batch_failed_commit_may_be_unknown";save();return fail("gc_batch_failed")}
		state["scan_complete"]=r.Complete;cursor=r.NextCursor;totalScanned+=int64(r.Scanned);totalEligible+=int64(r.Eligible);state["eligible"]=totalEligible;totalDeleted+=r.Deleted;totalAudits+=r.AuditDeleted;batches++
		for k,v:=range r.Retained{retained[k]+=v};state["scanned"]=totalScanned;state["deleted"]=totalDeleted;state["audit_deleted"]=totalAudits;state["retained"]=retained;state["confirmed_cursor"]=cursor;state["remaining_upper_id_span"]=maxID-cursor
		if e=save();e!=nil{return fail("report_write_failed")}
		if apply && (batches%checkpointEvery==0 || r.Complete){cc,done:=context.WithTimeout(ctx,30*time.Second);_,_,_,x=s.CheckpointHistoryGC(cc);done();if x!=nil{state["status"]="failed";state["error"]="checkpoint_failed";save();return fail("checkpoint_failed")}}
		if r.Complete || (maxBatches>0 && batches>=maxBatches){break}
	}
	if apply{
		state["stage"]="incremental_reclaim";if e=save();e!=nil{return fail("report_write_failed")}
		for{r,x:=s.ReclaimPages(ctx,store.PageReclaimRequest{DatabaseDevice:dev,DatabaseInode:ino},store.PageReclaimOptions{MaxPages:pages,MinFreePages:1,ReadBudget:2*time.Second,WriteBudget:reclaim});if x!=nil{state["status"]="failed";state["error"]="reclaim_failed";save();return fail("reclaim_failed")};state["reclaim_last"]=r;if e=save();e!=nil{return fail("report_write_failed")};if r.FreelistPagesRemoved==0{break};cc,done:=context.WithTimeout(ctx,30*time.Second);_,_,_,x=s.CheckpointHistoryGC(cc);done();if x!=nil{state["status"]="failed";state["error"]="checkpoint_failed";save();return fail("checkpoint_failed")}}
	}
	state["status"]="complete";if !apply{state["status"]="preview_complete"};state["stage"]="done";if e=save();e!=nil{return fail("report_write_failed")};summary,_:=json.Marshal(map[string]any{"status":state["status"],"scanned":totalScanned,"eligible":totalEligible,"deleted":totalDeleted,"audit_deleted":totalAudits});fmt.Println(string(summary));return 0
}
func fail(code string)int{b,_:=json.Marshal(map[string]string{"status":"failed","error":code});fmt.Println(string(b));return 1}
