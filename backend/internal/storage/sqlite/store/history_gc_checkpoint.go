package store

import("context";"errors")

// CheckpointHistoryGC is application-owned physical flush after bounded offline
// delete/reclaim transactions. Busy checkpoints retain the WAL and fail clearly.
func(s *Store) CheckpointHistoryGC(ctx context.Context)(busy,log,checkpointed int64,err error){
	if s.readOnly{return 0,0,0,errors.New("read_only_store")}
	if err=s.writeMu.LockContext(ctx);err!=nil{return};defer s.writeMu.Unlock()
	err=s.writeDB.QueryRowContext(ctx,`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy,&log,&checkpointed)
	if err==nil && busy!=0{err=errors.New("gc_checkpoint_busy")};return
}
