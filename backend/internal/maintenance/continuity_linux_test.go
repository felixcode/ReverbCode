//go:build linux

package maintenance

import (
 "context"
 "database/sql"
 "encoding/json"
 "fmt"
 "net"
 "net/http"
 "os"
 "path/filepath"
 "strings"
 "testing"
 "time"
 adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// Real runtime Unix handler, same Store admission and real modernc pager.
// The fixture is private; its final checkpoint is test-only, never online policy.
func TestContinuityUnixReclaimMultipleConfirmedTransactions(t *testing.T){
 dir:=t.TempDir();path:=filepath.Join(dir,"ao.db")
 w,e:=sql.Open("sqlite",path+"?_pragma=auto_vacuum(2)&_pragma=busy_timeout(0)");if e!=nil{t.Fatal(e)};defer w.Close();w.SetMaxOpenConns(1)
 if _,e=w.Exec(`CREATE TABLE goose_db_version(version_id INTEGER,is_applied INTEGER);INSERT INTO goose_db_version VALUES(192,1);CREATE TABLE payloads(id INTEGER PRIMARY KEY,body BLOB)`);e!=nil{t.Fatal(e)}
 if e=adapter.EnsureHistoryRetentionSchema(w);e!=nil{t.Fatal(e)}
 tx,e:=w.Begin();if e!=nil{t.Fatal(e)}
 for id:=0;id<768;id++{if _,e=tx.Exec(`INSERT INTO payloads VALUES(?,zeroblob(8192))`,id);e!=nil{t.Fatal(e)}}
 if e=tx.Commit();e!=nil{t.Fatal(e)}
 if _,e=w.Exec(`DELETE FROM payloads WHERE id<>767;PRAGMA journal_mode=WAL;PRAGMA wal_checkpoint(TRUNCATE)`);e!=nil{t.Fatal(e)}
 before,e:=os.Stat(path);if e!=nil{t.Fatal(e)}
 r,e:=sql.Open("sqlite",path+"?mode=ro&_pragma=busy_timeout(0)");if e!=nil{t.Fatal(e)};defer r.Close()
 s:=adapter.NewStore(w,r)
 parent:=filepath.Join(dir,"private");if e=os.Mkdir(parent,0700);e!=nil{t.Fatal(e)}
 socket:=filepath.Join(parent,"api.sock");o:=adapter.DefaultHistoryMaintenanceOptions();o.ClosedOwnerRetention=24*time.Hour
 srv,e:=Start(socket,s,o);if e!=nil{t.Fatal(e)};defer srv.Close()
 client:=&http.Client{Timeout:3*time.Second,Transport:&http.Transport{DialContext:func(ctx context.Context,_,_ string)(net.Conn,error){return (&net.Dialer{}).DialContext(ctx,"unix",socket)}}}
 defer client.CloseIdleConnections()
 res,e:=client.Get("http://localhost/v1/capability");if e!=nil{t.Fatal(e)}
 cap:=map[string]any{};e=json.NewDecoder(res.Body).Decode(&cap);res.Body.Close();if e!=nil||cap["closedOwnerRetentionHours"]!=float64(24){t.Fatal(cap,e)}
 request:=fmt.Sprintf(`{"database_device":%.0f,"database_inode":%.0f,"max_pages":16,"min_free_pages":256,"write_budget_ns":250000000}`,cap["dbDevice"],cap["dbInode"])
 began:=time.Now();confirmed:=0;var removed int64;var maxHeld,maxReserved float64
 for attempts:=0;attempts<512&&time.Since(began)<60*time.Second;attempts++{
  res,e=client.Post("http://localhost/v1/history/reclaim","application/json",strings.NewReader(request));if e!=nil{t.Fatal(e)}
  var result adapter.PageReclaimResult;e=json.NewDecoder(res.Body).Decode(&result);res.Body.Close();if e!=nil||res.StatusCode!=200{t.Fatal(result,e)}
  if result.Status=="no_work"{break}
  if !result.Committed||result.CommitState!="committed"||result.AttemptedSteps>16{t.Fatal(result)}
  confirmed++;removed+=result.LogicalPageBytesRemoved
  if result.HeldMS>maxHeld{maxHeld=result.HeldMS};if result.WriterReservedMS>maxReserved{maxReserved=result.WriterReservedMS}
 }
 if confirmed<=16||removed<=256*4096{t.Fatalf("did not exceed old per-run cap requests%d bytes%d",confirmed,removed)}
 // Read pool holds no snapshot. This is only proof of how private logical
 // pages materialize after a checkpoint; production never forces this here.
 if _,e=w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`);e!=nil{t.Fatal(e)}
 after,e:=os.Stat(path);if e!=nil{t.Fatal(e)};if before.Size()<=after.Size(){t.Fatalf("no physical shrink %d %d",before.Size(),after.Size())}
 var count,size int;if e=w.QueryRow(`SELECT count(*),length(body) FROM payloads WHERE id=767`).Scan(&count,&size);e!=nil||count!=1||size!=8192{t.Fatal(count,size,e)}
 t.Logf("CONFIRMED requests=%d logical_bytes=%d private_checkpoint_main_shrink=%d elapsed_ms=%d actual_max_held_ms=%.3f actual_max_reserved_ms=%.3f",confirmed,removed,before.Size()-after.Size(),time.Since(began).Milliseconds(),maxHeld,maxReserved)
}
