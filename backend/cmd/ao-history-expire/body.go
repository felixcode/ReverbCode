//go:build linux
package main

import("context";"errors";"strings";"time";"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store")

type bodySettings struct{Stages string;Cutoff,AsOf,CoverageBefore time.Time;MaxEventID int64;Device,Inode uint64;Rows int;Bytes,MaxRow int64;Budget time.Duration;MaxBatches int;MaxMarkers int64;StartCursor string}

// Existing application policy, preparation/CAS, replay fences and transaction
// handling own every body change. This is only an offline bounded caller.
func runBodyStages(ctx context.Context,s *store.Store,o bodySettings,state map[string]any,save func()error)error{
 allowed:=map[string]bool{"gc:raw":true,"raw":true,"activity":true,"outputs":true,"message":true,"turn":true,"session":true};seen:=map[string]bool{};stages:=strings.Split(o.Stages,",")
 for _,name:=range stages{if !allowed[name]||seen[name]{return errors.New("invalid_body_stage")};seen[name]=true}
 all:=map[string]any{};state["body_stages"]=all;state["body_as_of"]=o.AsOf.Format(time.RFC3339Nano);state["body_logical_bytes_removed"]=int64(0);state["body_compacted"]=0;state["body_deleted"]=0;state["body_audits_deleted"]=0;state["body_markers_inserted"]=int64(0)
 options:=store.DefaultHistoryMaintenanceOptions();options.Retention=48*time.Hour;options.BatchRows=o.Rows;options.BatchBytes=o.Bytes;options.MaxRowBytes=o.MaxRow;options.ReadBudget=o.Budget;options.WriteBudget=o.Budget
 for _,stage:=range stages{
  if stage=="outputs" && seen["activity"]{all[stage]=map[string]any{"status":"subsumed_by_activity_all_four_fields"};continue}
  result:=map[string]any{"scanned":0,"compacted":0,"deleted":0,"audit_deleted":0,"markers_inserted":int64(0),"logical_bytes_removed":int64(0),"confirmed_cursor":"","complete":false,"retained":map[string]int{}}
  all[stage]=result;state["stage"]="body:"+stage;cursor:=o.StartCursor;o.StartCursor="";batches:=0
  for{
   left:=o.MaxMarkers-state["body_markers_inserted"].(int64);if left<=0{state["status"]="partial";state["body_stop_reason"]="new_marker_budget";return nil};options.BatchRows=min(o.Rows,int(left))
   request:=store.HistoryMaintenanceRequest{Stage:stage,Cursor:cursor,Cutoff:o.Cutoff.Format(time.RFC3339Nano),DatabaseDevice:o.Device,DatabaseInode:o.Inode,OfflineCoverageBefore:o.CoverageBefore,OfflineMaxEventID:o.MaxEventID}
   parent,done:=context.WithTimeout(ctx,2*o.Budget);r,e:=s.MaintainHistorySnapshotBatch(parent,request,options,o.AsOf);done()
   if e!=nil{result["failed_phase"]=r.Phase;result["failed_commit_state"]=r.CommitState;result["error_type"]=r.ErrorType;result["sqlite_code"]=r.SQLiteCode;state["status"]="failed";state["error"]="body_batch_failed";save();return e}
   if r.Status=="deferred"{state["status"]="partial";state["error"]="body_admission_deferred";save();return errors.New("body_admission_deferred")}
   // The daemon returns the original cursor/zero change counts on failure.
   // Only a confirmed commit (or a read-only end-of-stage) advances this prefix.
   cursor=r.NextCursor;batches++;result["confirmed_cursor"]=cursor;result["scanned"]=result["scanned"].(int)+r.Scanned;result["compacted"]=result["compacted"].(int)+r.Compacted;result["markers_inserted"]=result["markers_inserted"].(int64)+r.Markers;result["logical_bytes_removed"]=result["logical_bytes_removed"].(int64)+r.LogicalBytes;result["complete"]=r.Complete;result["deleted"]=result["deleted"].(int)+r.Deleted;result["audit_deleted"]=result["audit_deleted"].(int)+r.AuditDeleted;state["body_deleted"]=state["body_deleted"].(int)+r.Deleted;state["body_audits_deleted"]=state["body_audits_deleted"].(int)+r.AuditDeleted
   result["last_read_ms"]=r.ReadMS;result["last_held_ms"]=r.HeldMS;result["last_commit_state"]=r.CommitState
   retained:=result["retained"].(map[string]int);for k,v:=range r.Retained{retained[k]+=v}
   state["body_compacted"]=state["body_compacted"].(int)+r.Compacted;state["body_markers_inserted"]=state["body_markers_inserted"].(int64)+r.Markers;state["body_logical_bytes_removed"]=state["body_logical_bytes_removed"].(int64)+r.LogicalBytes
   if e=save();e!=nil{return e}
   // Keep WAL bounded without altering the application transaction or recovery
   // audit policy. Existing proofs skip before body parsing/materialization.
   if r.Compacted>0 || r.Markers>0 || r.Deleted>0 || r.AuditDeleted>0 {cc,stop:=context.WithTimeout(ctx,o.Budget);_,_,_,e=s.CheckpointHistoryGC(cc);stop();if e!=nil{state["status"]="failed";state["error"]="body_checkpoint_failed";save();return e}}
   if r.Complete{break};if o.MaxBatches>0&&batches>=o.MaxBatches{state["status"]="partial";state["body_stop_reason"]="max_batches";return nil}
   if ctx.Err()!=nil{return ctx.Err()}
  }
 }
 state["body_complete"]=true;state["body_stage_count"]=len(stages);return nil
}
