package store_test

import (
 "context"
 "crypto/sha256"
 "database/sql"
 "database/sql/driver"
 "errors"
 "fmt"
 "os"
 "path/filepath"
 "strings"
 "sync"
 "sync/atomic"
 "testing"
 "time"

 "github.com/aoagents/agent-orchestrator/backend/internal/domain"
 devimportsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/devimport"
 "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
 "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
 sqliteDriver "modernc.org/sqlite"
)

// Observe real driver rows rather than exposing a test hook in the store.
type retentionObservedDriver struct{afterMarker func();markerRows *atomic.Int64}
func(d retentionObservedDriver)Open(name string)(driver.Conn,error){c,e:=(&sqliteDriver.Driver{}).Open(name);if e!=nil{return nil,e};return &retentionObservedConn{Conn:c,owner:d},nil}
type retentionObservedConn struct{driver.Conn;owner retentionObservedDriver}
func(c *retentionObservedConn)QueryContext(ctx context.Context,q string,args []driver.NamedValue)(driver.Rows,error){
 rows,e:=c.Conn.(driver.QueryerContext).QueryContext(ctx,q,args);if e!=nil{return nil,e}
 callback:=func(){}
 if strings.HasPrefix(strings.TrimSpace(q),"SELECT EXISTS(SELECT 1 FROM ao_history_retention r")&&c.owner.afterMarker!=nil{callback=c.owner.afterMarker}
 var count *atomic.Int64;if strings.Contains(q,"SELECT object_kind,object_id,semantic_sha256,prompt_sha256 FROM ao_history_retention"){count=c.owner.markerRows}
 return &retentionObservedRows{Rows:rows,after:callback,count:count},nil
}
func(c *retentionObservedConn)ExecContext(ctx context.Context,q string,args []driver.NamedValue)(driver.Result,error){return c.Conn.(driver.ExecerContext).ExecContext(ctx,q,args)}
type retentionObservedRows struct{driver.Rows;after func();count *atomic.Int64;closed bool}
func(r *retentionObservedRows)Close()error{e:=r.Rows.Close();if !r.closed{r.closed=true;r.after()};return e}
func(r *retentionObservedRows)Next(values []driver.Value)error{e:=r.Rows.Next(values);if e==nil&&r.count!=nil{r.count.Add(1)};return e}
var retentionDriverID atomic.Int32

func retentionPoolFixture(t *testing.T,observed *retentionObservedDriver)(*sqlite.Store,*sql.DB,*sql.DB,string,domain.SessionID,string){
 t.Helper();ctx:=context.Background();dir:=t.TempDir();initial,e:=sqlite.Open(dir);if e!=nil{t.Fatal(e)};if e=initial.Close();e!=nil{t.Fatal(e)}
 dsn:="file:"+filepath.Join(dir,"ao.db")+"?_pragma=busy_timeout(1000)&_pragma=foreign_keys(ON)"
 writer,e:=sql.Open("sqlite",dsn);if e!=nil{t.Fatal(e)};writer.SetMaxOpenConns(1)
 name:="sqlite";if observed!=nil{name=fmt.Sprintf("retention-boundary-%d",retentionDriverID.Add(1));sql.Register(name,*observed)}
 reader,e:=sql.Open(name,dsn);if e!=nil{t.Fatal(e)};reader.SetMaxOpenConns(1);reader.SetMaxIdleConns(1)
 s:=store.NewStore(writer,reader);t.Cleanup(func(){_ =s.Close()})
 seedProject(t,s,"bounded");rec:=sampleRecord("bounded");rec.Mode=domain.SessionModeChat;session,e:=s.CreateSession(ctx,rec);if e!=nil{t.Fatal(e)}
 conv,e:=s.CreateConversation(ctx,"bounded-conversation",domain.ConversationScopeProject,"bounded",session.ID,histClock);if e!=nil{t.Fatal(e)}
 if e=s.ClaimChatControllerGeneration(ctx,session.ID,"gen-1");e!=nil{t.Fatal(e)}
 msg:=domain.ConversationMessage{ID:"bounded-message",Text:"original prompt",Origin:domain.MessageOriginHuman,ClientMessageID:"bounded-client",DeliveryContentJSON:`[{"type":"image","data":"AQ==","mimeType":"image/png"}]`}
 if _,e=s.AppendUserMessage(ctx,conv.ID,session.ID,"gen-1",msg,"bounded-turn",histClock);e!=nil{t.Fatal(e)}
 if e=s.BindTurnToProvider(ctx,"bounded-turn","bounded-provider",histClock);e!=nil{t.Fatal(e)}
 if e=s.SettleTurnByID(ctx,"bounded-turn",domain.TurnStateFailed,"failed",histClock);e!=nil{t.Fatal(e)}
 return s,writer,reader,conv.ID,session.ID,dir
}

func TestRetentionSingleConnectionSnapshotsAndConcurrentReaders(t *testing.T){
 s,_,reader,conv,_,_:=retentionPoolFixture(t,nil)
 for _,page:=range []bool{false,true}{
  t.Run(fmt.Sprintf("page=%v",page),func(t *testing.T){
   start:=make(chan struct{});results:=make(chan error,8);var wg sync.WaitGroup
   for i:=0;i<8;i++{wg.Add(1);go func(){defer wg.Done();<-start;ctx,cancel:=context.WithTimeout(context.Background(),2*time.Second);defer cancel();if page{_,e:=s.LoadConversationSnapshotPage(ctx,conv,0,1);results<-e}else{_,e:=s.LoadConversationSnapshot(ctx,conv);results<-e}}()}
   close(start);wg.Wait();close(results);for e:=range results{if e!=nil{t.Fatalf("snapshot recursively acquired pool connection: %v",e)}}
   if reader.Stats().InUse!=0{t.Fatal("snapshot leaked its read connection")}
  })
 }
}

func TestRetentionRetryAndEditReadMarkerAndBodyAtomically(t *testing.T){
 for _,edit:=range []bool{false,true}{t.Run(fmt.Sprintf("edit=%v",edit),func(t *testing.T){
  var writer *sql.DB;var fired atomic.Bool;var hookErr error
  observer:=&retentionObservedDriver{afterMarker:func(){if fired.CompareAndSwap(false,true){
   tx,e:=writer.Begin();if e!=nil{hookErr=e;return};defer tx.Rollback()
   _,e=tx.Exec(`UPDATE conversation_messages SET text='[AO history expired]',delivery_content_json='' WHERE id='bounded-message'`);if e==nil{_,e=tx.Exec(`INSERT INTO ao_history_retention VALUES('message','bounded-message','bounded-conversation',?, ?,8,?,'2026-10-25','2026-10-23','["text"]')`,retentionHash("user\x00original prompt"),retentionHash("original prompt"),retentionHash("body"))};if e==nil{e=tx.Commit()};hookErr=e
  }}}
  s,w,_,conv,_,_:=retentionPoolFixture(t,observer);writer=w
  if edit{
   anchor,e:=s.ConversationEditAnchor(context.Background(),conv,"bounded-turn");if e!=nil{t.Fatal(e)}
   if anchor.OriginalDeliveryContentJSON==""{t.Fatal("mixed marker/body snapshots returned cleared anchor")}
   if _,e=s.ConversationEditAnchor(context.Background(),conv,"bounded-turn");!errors.Is(e,domain.ErrHistoryContentExpired){t.Fatalf("subsequent expired edit accepted: %v",e)}
  }else{
   prompt,e:=s.RetryPrompt(context.Background(),conv,"bounded-turn");if e!=nil{t.Fatal(e)}
   if prompt.Text!="original prompt"{t.Fatal("mixed marker/body snapshots returned an expiry stub")}
   if _,e=s.RetryPrompt(context.Background(),conv,"bounded-turn");!errors.Is(e,domain.ErrHistoryContentExpired){t.Fatalf("subsequent expired retry accepted: %v",e)}
  }
  if !fired.Load()||hookErr!=nil{t.Fatalf("controlled retention interleaving absent: fired=%v err=%v",fired.Load(),hookErr)}
 })}
}

func TestRetentionPageLoadsOnlyItsProjectionMarkers(t *testing.T){
 var returned atomic.Int64
 s,w,_,conv,_,_:=retentionPoolFixture(t,&retentionObservedDriver{markerRows:&returned})
 tx,e:=w.Begin();if e!=nil{t.Fatal(e)};defer tx.Rollback()
 insert,e:=tx.Prepare(`INSERT INTO ao_history_retention VALUES(?,?,?,'','',0,'','2026-10-25','2026-10-23','[]')`);if e!=nil{t.Fatal(e)}
 for _,kind:=range []string{"provider_event","activity_output","session"}{for i:=0;i<2000;i++{if _,e=insert.Exec(kind,fmt.Sprintf("unrelated-%d",i),conv);e!=nil{t.Fatal(e)}}};insert.Close()
 if _,e=tx.Exec(`UPDATE conversation_messages SET text='[AO history expired]' WHERE id='bounded-message'`);e!=nil{t.Fatal(e)}
 for _,kind:=range []string{"message","message_coordination"}{if _,e=tx.Exec(`INSERT INTO ao_history_retention VALUES(?,'bounded-message',?,?, ?,8,'','2026-10-25','2026-10-23','["text"]')`,kind,conv,retentionHash("user\x00original prompt"),retentionHash("original prompt"));e!=nil{t.Fatal(e)}}
 if e=tx.Commit();e!=nil{t.Fatal(e)}
 for _,page:=range []bool{true,false}{returned.Store(0);var snapshot store.ConversationSnapshot
  if page{snapshot,e=s.LoadConversationSnapshotPage(context.Background(),conv,0,1)}else{snapshot,e=s.LoadConversationSnapshot(context.Background(),conv)}
  if e!=nil{t.Fatal(e)};if len(snapshot.Messages)!=1||!snapshot.Messages[0].ContentExpired||!snapshot.Messages[0].RetentionCoordination{t.Fatal("page lost required exact-ID markers")}
  if rows:=returned.Load();rows!=2{t.Fatalf("snapshot loaded unrelated audit markers: %d",rows)}
 }
}

func TestRetentionLegacyReadOnlySourceRemainsUnmodifiedAndImportable(t *testing.T){
 s,w,_,conv,session,dir:=retentionPoolFixture(t,nil)
 // The new sidecar is the only schema delta from the exact installed migration
 // ledger. Removing it constructs a genuine unmarked 800c source in this fixture.
 if _,e:=w.Exec(`DROP TABLE ao_history_retention`);e!=nil{t.Fatal(e)}
 if e:=s.Close();e!=nil{t.Fatal(e)}
 sourcePath:=filepath.Join(dir,"ao.db");before,e:=os.ReadFile(sourcePath);if e!=nil{t.Fatal(e)};sourceHash:=sha256.Sum256(before)
 ro,e:=sqlite.OpenReadOnly(context.Background(),dir);if e!=nil{t.Fatal(e)}
 for i:=0;i<12;i++{
  record,found,e:=ro.GetSession(context.Background(),session);if e!=nil||!found||record.Metadata.PromptExpired{t.Fatalf("legacy GetSession: %v",e)}
  if _,e=ro.ListSessions(context.Background(),"bounded");e!=nil{t.Fatal(e)}
  if _,e=ro.ListAllSessions(context.Background());e!=nil{t.Fatal(e)}
  full,e:=ro.LoadConversationSnapshot(context.Background(),conv);if e!=nil||len(full.Messages)!=1||full.Messages[0].ContentExpired{t.Fatalf("legacy snapshot: %v",e)}
  if _,e=ro.LoadConversationSnapshotPage(context.Background(),conv,0,1);e!=nil{t.Fatal(e)}
 }
 if e=ro.Close();e!=nil{t.Fatal(e)}
 targetDir:=t.TempDir();target,e:=sqlite.Open(targetDir);if e!=nil{t.Fatal(e)};defer target.Close()
 manager:=devimportsvc.New(devimportsvc.Deps{Store:target,TargetDataDir:targetDir,OpenSource:func(ctx context.Context,path string)(devimportsvc.SourceStore,error){return sqlite.OpenReadOnly(ctx,path)}})
 report,e:=manager.RunProjects(context.Background(),devimportsvc.RunInput{SourceDataDir:dir,DryRun:true});if e!=nil||report.Inserted!=1{t.Fatalf("legacy dev import: %v",e)}
 after,e:=os.ReadFile(sourcePath);if e!=nil{t.Fatal(e)};if sha256.Sum256(after)!=sourceHash{t.Fatal("read-only compatibility modified old source")}
 verify,e:=sql.Open("sqlite","file:"+sourcePath+"?mode=ro");if e!=nil{t.Fatal(e)};defer verify.Close();var present int
 if e=verify.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='ao_history_retention'`).Scan(&present);e!=nil||present!=0{t.Fatal("read-only source schema was initialized")}
}
