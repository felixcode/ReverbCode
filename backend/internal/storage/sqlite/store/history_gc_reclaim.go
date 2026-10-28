package store

import("context";"database/sql";"errors";"strconv")

// HistoryGCOfflineReclaimResult describes only a confirmed offline transaction.
// Filesystem bytes are observed separately after the caller checkpoints the WAL.
type HistoryGCOfflineReclaimResult struct {
 BeforePages int64 `json:"before_pages"`
 BeforeFree int64 `json:"before_free"`
 AfterPages int64 `json:"after_pages"`
 AfterFree int64 `json:"after_free"`
 FreelistPagesRemoved int64 `json:"freelist_pages_removed"`
 LogicalPagesRemoved int64 `json:"logical_pages_removed"`
 LogicalBytesRemoved int64 `json:"logical_bytes_removed"`
 ResultRows int64 `json:"result_rows"`
 CommitState string `json:"commit_state"`
 Committed bool `json:"transaction_committed"`
}
func historyGCDrainVacuum(ctx context.Context,tx *sql.Tx,pages int64)(int64,error){
 if pages<=0{return 0,errors.New("invalid_offline_reclaim_pages")}
 rows,e:=tx.QueryContext(ctx,`PRAGMA main.incremental_vacuum(`+strconv.FormatInt(pages,10)+`)`);if e!=nil{return 0,e}
 var n int64;for rows.Next(){n++};e=rows.Err();closed:=rows.Close();if e==nil{e=closed};return n,e
}
// ReclaimHistoryGCOffline is deliberately separate from online ReclaimPages.
// It requires the offline CLI's stopped-daemon/identity/maintenance-lock guards.
// One fully drained SQLite statement replaces N Go/SQL round trips.
func(s *Store) ReclaimHistoryGCOffline(ctx context.Context,pages int64)(r HistoryGCOfflineReclaimResult,err error){
 r.CommitState="not_started"
 defer func(){if err!=nil{r.FreelistPagesRemoved=0;r.LogicalPagesRemoved=0;r.LogicalBytesRemoved=0;r.Committed=false}}()
 if s.readOnly||pages<=0{return r,errors.New("invalid_offline_reclaim_options")}
 if err=s.writeMu.LockContext(ctx);err!=nil{return r,err};defer s.writeMu.Unlock()
 tx,err:=s.writeDB.BeginTx(ctx,nil);if err!=nil{return r,err};r.CommitState="open"
 defer func(){e:=tx.Rollback();if r.CommitState=="open"{if e==nil{r.CommitState="rolled_back"}else{r.CommitState="unknown"}}}()
 var auto int;var size int64
 for query,dst:=range map[string]any{"PRAGMA main.auto_vacuum":&auto,"PRAGMA main.page_count":&r.BeforePages,"PRAGMA main.freelist_count":&r.BeforeFree,"PRAGMA main.page_size":&size}{if err=tx.QueryRowContext(ctx,query).Scan(dst);err!=nil{return r,err}}
 if auto!=2{return r,errors.New("offline_reclaim_requires_incremental")}
 r.AfterPages=r.BeforePages;r.AfterFree=r.BeforeFree
 if r.BeforeFree==0{return r,nil};pages=min(pages,r.BeforeFree)
 r.ResultRows,err=historyGCDrainVacuum(ctx,tx,pages);if err!=nil{return r,err}
 if err=tx.QueryRowContext(ctx,`PRAGMA main.page_count`).Scan(&r.AfterPages);err!=nil{return r,err}
 if err=tx.QueryRowContext(ctx,`PRAGMA main.freelist_count`).Scan(&r.AfterFree);err!=nil{return r,err}
 if r.AfterFree>r.BeforeFree||r.AfterPages>r.BeforePages||r.BeforeFree-r.AfterFree>pages{return r,errors.New("offline_reclaim_invalid_delta")}
 r.CommitState="unknown";if err=tx.Commit();err!=nil{return r,err};r.CommitState="committed";r.Committed=true
 r.FreelistPagesRemoved=r.BeforeFree-r.AfterFree;r.LogicalPagesRemoved=r.BeforePages-r.AfterPages;r.LogicalBytesRemoved=r.LogicalPagesRemoved*size;return r,nil
}
