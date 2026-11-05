package sqlite

import (
 "context"
 "database/sql"
 "os"
 "path/filepath"
 "syscall"
 "testing"
 "time"

 sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func offlineGCFixture(t *testing.T, version int64) (string, uint64, uint64) {
 t.Helper()
 dir := t.TempDir()
 path := filepath.Join(dir, "ao.db")
 if err := os.WriteFile(path, migratedDatabaseSnapshot(t, version), 0600); err != nil { t.Fatal(err) }
 db, err := sql.Open("sqlite", "file:"+path)
 if err != nil { t.Fatal(err) }
 if _, err = db.Exec(`PRAGMA auto_vacuum=INCREMENTAL; VACUUM`); err != nil { t.Fatal(err) }
 if err = sqlitestore.EnsureHistoryRetentionSchema(db); err != nil { t.Fatal(err) }
 if err = db.Close(); err != nil { t.Fatal(err) }
 info, err := os.Stat(path); if err != nil { t.Fatal(err) }
 st := info.Sys().(*syscall.Stat_t)
 return dir, uint64(st.Dev), st.Ino
}

func TestOfflineGCReviewedSchemasPreviewAndApply(t *testing.T) {
 for _, version := range []int64{192, 20261028124744441} {
  t.Run(time.Unix(version,0).Format("20060102150405"),func(t *testing.T){
   dir, dev, ino := offlineGCFixture(t,version)
   for _, apply := range []bool{false,true} {
    s, err := OpenHistoryGCExisting(context.Background(),dir,dev,ino,apply)
    if err != nil { t.Fatalf("version %d apply %v: %v",version,apply,err) }
    opts := sqlitestore.HistoryGCOptions{Cutoff:time.Now().UTC().Add(-49*time.Hour),BatchRows:10,Apply:true}
    r, pruneErr := s.PruneAnonymousHistoryBatch(context.Background(),0,opts)
    if !apply && (pruneErr == nil || pruneErr.Error() != "read_only_store") { t.Fatalf("preview accepted write: %v",pruneErr) }
    if apply && (pruneErr != nil || !r.Committed || r.Deleted != 0) { t.Fatalf("empty apply result: %+v %v",r,pruneErr) }
    if err = s.Close();err !=nil { t.Fatal(err) }
   }
  })
 }
}

func TestOfflineGCRejectsUnreviewedOrIncompleteSchema(t *testing.T) {
 for _, tc := range []struct{name,mutation,want string}{
  {"unknown_version",`INSERT INTO goose_db_version(version_id,is_applied,tstamp) VALUES(20261028124744442,1,CURRENT_TIMESTAMP)`,"gc_requires_reviewed_schema_incremental"},
  {"missing_current_shape",`ALTER TABLE sessions RENAME COLUMN runtime_released_launch_id TO unavailable_launch`,"gc_maintenance_schema_shape"},
  {"unexpected_marker_column",`ALTER TABLE ao_history_retention ADD COLUMN unknown TEXT`,"gc_retention_schema_shape"},
 } {
  t.Run(tc.name,func(t *testing.T){
   dir,dev,ino := offlineGCFixture(t,20261028124744441)
   db,err := sql.Open("sqlite",filepath.Join(dir,"ao.db"));if err !=nil {t.Fatal(err)}
   if _,err=db.Exec(tc.mutation);err!=nil {t.Fatal(err)};db.Close()
   s,err := OpenHistoryGCExisting(context.Background(),dir,dev,ino,true)
   if s !=nil {s.Close()};if err ==nil || err.Error()!=tc.want {t.Fatalf("got %v want %s",err,tc.want)}
  })
 }
}
