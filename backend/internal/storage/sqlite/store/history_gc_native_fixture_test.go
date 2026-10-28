package store_test
import("context";"database/sql";"encoding/json";"os";"path/filepath";"testing";"time";"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite")
// Test-only export of a tiny Go-created full AO schema192 database for native
// engine format compatibility. Never opens a production path or copies a DB.
func TestHistoryGCNativeFixtureExport(t *testing.T){
 dir:=os.Getenv("AO_NATIVE_RECLAIM_FIXTURE_OUT");if dir==""{t.Skip("test-only fixture output not requested")};if e:=os.Mkdir(dir,0700);e!=nil{t.Fatal(e)};path:=filepath.Join(dir,"ao.db")
 w,e:=sql.Open("sqlite",path+"?_pragma=auto_vacuum(2)");if e!=nil{t.Fatal(e)};if _,e=w.Exec(`CREATE TABLE native_reclaim_fixture(id INTEGER PRIMARY KEY,body BLOB)`);e!=nil{t.Fatal(e)};w.Close()
 s,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};w,e=sql.Open("sqlite",path);if e!=nil{t.Fatal(e)}
 tx,e:=w.Begin();if e!=nil{t.Fatal(e)};for i:=0;i<128;i++{if _,e=tx.Exec(`INSERT INTO native_reclaim_fixture VALUES(?,zeroblob(8192))`,i);e!=nil{t.Fatal(e)}};if e=tx.Commit();e!=nil{t.Fatal(e)};if _,e=w.Exec(`DELETE FROM native_reclaim_fixture WHERE id<127`);e!=nil{t.Fatal(e)}
 var before,after int64;w.QueryRow(`PRAGMA freelist_count`).Scan(&before);tx,e=w.BeginTx(context.Background(),nil);if e!=nil{t.Fatal(e)};started:=time.Now();rows,e:=tx.Query(`PRAGMA incremental_vacuum(16)`);if e!=nil{t.Fatal(e)};for rows.Next(){};if e=rows.Err();e!=nil{t.Fatal(e)};rows.Close();elapsed:=time.Since(started);tx.QueryRow(`PRAGMA freelist_count`).Scan(&after);if before-after!=16{t.Fatal("Go fixture reclaim delta",before,after)};tx.Rollback()
 w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`);w.Close();s.Close();os.Chmod(path,0600)
 report:=map[string]any{"go_sqlite":"modernc_source_dependency","go_16_pages_ms":float64(elapsed)/float64(time.Millisecond),"schema":192,"free_before":before,"free_inside":after,"rollback":true};data,_:=json.Marshal(report);if e=os.WriteFile(filepath.Join(dir,"fixture-safe.json"),data,0600);e!=nil{t.Fatal(e)}
}
func TestHistoryGCNativeFixtureReadback(t *testing.T){
 path:=os.Getenv("AO_NATIVE_RECLAIM_READBACK");if path==""{t.Skip("test-only native-written fixture not requested")};w,e:=sql.Open("sqlite","file:"+path+"?mode=ro");if e!=nil{t.Fatal(e)};defer w.Close();var version,count,id,size int;if e=w.QueryRow(`SELECT max(version_id) FROM goose_db_version WHERE is_applied=1`).Scan(&version);e!=nil||version!=192{t.Fatal("schema roundtrip",version,e)};if e=w.QueryRow(`SELECT count(*),id,length(body) FROM native_reclaim_fixture`).Scan(&count,&id,&size);e!=nil||count!=1||id!=127||size!=8192{t.Fatal("payload roundtrip",count,id,size,e)}
}
