package store

import("context";"database/sql";"testing";"time";_ "modernc.org/sqlite")

func gcFixture(t *testing.T)(*Store,*sql.DB,time.Time){
	t.Helper();db,e:=sql.Open("sqlite",":memory:");if e!=nil{t.Fatal(e)};db.SetMaxOpenConns(1);t.Cleanup(func(){db.Close()})
	_,e=db.Exec(`CREATE TABLE conversation_provider_events(id INTEGER PRIMARY KEY AUTOINCREMENT,conversation_id TEXT,provider_event_id TEXT,method TEXT,received_at TEXT);
 CREATE TABLE ao_history_retention(object_kind TEXT,object_id TEXT,expired_at TEXT,PRIMARY KEY(object_kind,object_id));
 CREATE TABLE conversation_turns(id TEXT,conversation_id TEXT,state TEXT,requested_at TEXT,started_at TEXT);
 CREATE TABLE conversation_activities(conversation_id TEXT,turn_id TEXT,kind TEXT,status TEXT,created_at TEXT);
 CREATE TABLE conversation_edit_deliveries(conversation_id TEXT,state TEXT,created_at TEXT);
 CREATE TABLE conversation_steer_deliveries(conversation_id TEXT,state TEXT,created_at TEXT);
 CREATE TABLE conversation_messages(id TEXT PRIMARY KEY,client_message_id TEXT);
 INSERT INTO conversation_messages VALUES('original','original-client');`);if e!=nil{t.Fatal(e)}
	return NewStore(db,db),db,time.Now().UTC()
}
func gcInsert(t *testing.T,db *sql.DB,id int,conv,handle,method,stamp string){t.Helper();if _,e:=db.Exec(`INSERT INTO conversation_provider_events VALUES(?,?,?,?,?)`,id,conv,handle,method,stamp);e!=nil{t.Fatal(e)}}
func TestHistoryGCDeletesOnlyCoveredConsumedAnonymousStreams(t *testing.T){
	s,db,now:=gcFixture(t);old:=now.Add(-72*time.Hour).Format(time.RFC3339Nano)
	gcInsert(t,db,1,"live","","message.delta",old);gcInsert(t,db,2,"live","","reasoning.delta",now.Format(time.RFC3339Nano));gcInsert(t,db,3,"live","native-key","message.delta",old);gcInsert(t,db,4,"live","","turn.completed",old);gcInsert(t,db,5,"pending","","message.delta",old);gcInsert(t,db,6,"live","","message.delta","bad-time");gcInsert(t,db,7,"live","","activity.text",old);gcInsert(t,db,100,"live","","message.delta",old)
	_,e:=db.Exec(`INSERT INTO conversation_turns VALUES('current','live','running',?,NULL),('pending','pending','running',?,NULL);INSERT INTO ao_history_retention VALUES('provider_event','1',?),('provider_event','7',?)`,now.Format(time.RFC3339Nano),now.Add(-96*time.Hour).Format(time.RFC3339Nano),now.Add(-24*time.Hour).Format(time.RFC3339Nano),now.Format(time.RFC3339Nano));if e!=nil{t.Fatal(e)}
	r,e:=s.PruneAnonymousHistoryBatch(context.Background(),0,HistoryGCOptions{Cutoff:now.Add(-48*time.Hour),CoverageBefore:now.Add(-time.Hour),MaxEventID:10,BatchRows:100,Apply:true});if e!=nil{t.Fatal(e)}
	if r.Deleted!=1 || r.AuditDeleted!=1 || !r.Committed{t.Fatalf("bad result %+v",r)}
	var n int;if e=db.QueryRow(`SELECT count(*) FROM conversation_provider_events`).Scan(&n);e!=nil||n!=7{t.Fatalf("remaining %d %v",n,e)}
	if e=db.QueryRow(`SELECT count(*) FROM conversation_messages WHERE client_message_id='original-client'`).Scan(&n);e!=nil||n!=1{t.Fatal("input identity changed")}
	if r.Retained["unresolved_tail"]!=1 || r.Retained["invalid_timestamp"]!=1 || r.Retained["audit_outside_backup"]!=1{t.Fatalf("protection missing %+v",r.Retained)}
}
func TestHistoryGCPreviewAndRollbackDoNotDeleteOrAdvanceFailedCursor(t *testing.T){
	s,db,now:=gcFixture(t);gcInsert(t,db,1,"c","","message.delta",now.Add(-72*time.Hour).Format(time.RFC3339Nano));db.Exec(`INSERT INTO ao_history_retention VALUES('provider_event','1','2020-01-01 00:00:00')`)
	o:=HistoryGCOptions{Cutoff:now.Add(-48*time.Hour),MaxEventID:10,BatchRows:10}
	r,e:=s.PruneAnonymousHistoryBatch(context.Background(),0,o);if e!=nil||r.Eligible!=1||r.Deleted!=0{t.Fatalf("preview %+v %v",r,e)}
	db.Exec(`CREATE TRIGGER reject_gc BEFORE DELETE ON conversation_provider_events BEGIN SELECT RAISE(ABORT,'synthetic');END`);o.Apply=true
	r,e=s.PruneAnonymousHistoryBatch(context.Background(),0,o);if e==nil || r.Committed || r.Deleted!=0 || r.NextCursor!=0{t.Fatalf("failed prefix advanced %+v %v",r,e)}
	var n int;db.QueryRow(`SELECT count(*) FROM ao_history_retention`).Scan(&n);if n!=1{t.Fatal("audit delete did not rollback")};db.QueryRow(`SELECT count(*) FROM conversation_provider_events`).Scan(&n);if n!=1{t.Fatal("event deleted")}
}
func TestHistoryGCUnknownBoundaryIsScopedAndRecentCutoffRejected(t *testing.T){
	s,db,now:=gcFixture(t);old:=now.Add(-72*time.Hour).Format(time.RFC3339Nano);gcInsert(t,db,1,"unknown","","message.delta",old);gcInsert(t,db,2,"other","","message.delta",old);db.Exec(`INSERT INTO conversation_turns VALUES('t','unknown','queued','bad',NULL)`)
	o:=HistoryGCOptions{Cutoff:now.Add(-48*time.Hour),MaxEventID:10,BatchRows:10,Apply:true};r,e:=s.PruneAnonymousHistoryBatch(context.Background(),0,o);if e!=nil||r.Deleted!=1||r.Retained["unknown_pending_boundary"]!=1{t.Fatalf("wrong scope %+v %v",r,e)}
	o.Cutoff=now.Add(-time.Hour);if _,e=s.PruneAnonymousHistoryBatch(context.Background(),0,o);e==nil{t.Fatal("recent cutoff accepted")}
}

func TestHistoryGCAdditionalProjectedAnonymousMethods(t *testing.T){
 s,db,now:=gcFixture(t);old:=now.Add(-72*time.Hour).Format(time.RFC3339Nano)
 for i,m:=range []string{"activity.started","activity.completed","usage","account.rateLimits"}{gcInsert(t,db,i+1,"c","",m,old);gcInsert(t,db,i+101,"c","native",m,old)}
 r,e:=s.PruneAnonymousHistoryBatch(context.Background(),0,HistoryGCOptions{Cutoff:now.Add(-48*time.Hour),BatchRows:1000,Apply:true});if e!=nil||r.Deleted!=4{t.Fatalf("additional projected methods %+v %v",r,e)}
 var n int;db.QueryRow(`SELECT count(*) FROM conversation_provider_events WHERE provider_event_id<>''`).Scan(&n);if n!=4{t.Fatal("native identity removed")}
}
