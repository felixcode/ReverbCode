package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
 "strconv"
	"syscall"

	sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// OpenHistoryGCExisting never creates a database, runs migrations or starts AO.
// The operator must hold the maintenance lock with the daemon stopped.
func OpenHistoryGCExisting(ctx context.Context, dataDir string, device,inode uint64, apply bool, offlineCacheMiB ...int) (*Store,error) {
	path:=filepath.Join(dataDir,"ao.db")
	before,err:=os.Lstat(path);if err!=nil{return nil,err}
	st,ok:=before.Sys().(*syscall.Stat_t)
	if !ok || !before.Mode().IsRegular() || before.Mode().Perm()!=0600 || st.Uid!=uint32(os.Getuid()) || st.Nlink!=1 || uint64(st.Dev)!=device || st.Ino!=inode{return nil,errors.New("gc_database_identity")}
	u:=url.URL{Scheme:"file",Path:path};ro:=u.String()+"?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(0)"
	check,err:=sql.Open("sqlite",ro);if err!=nil{return nil,err}
	var version,auto int
	err=check.QueryRowContext(ctx,`SELECT max(version_id) FROM goose_db_version WHERE is_applied=1`).Scan(&version)
	if err==nil{err=check.QueryRowContext(ctx,`PRAGMA auto_vacuum`).Scan(&auto)}
	check.Close();if err!=nil{return nil,err};if version!=192 || auto!=2{return nil,errors.New("gc_requires_schema192_incremental")}
	mode:="ro";pragma:="&_pragma=query_only(1)";if apply{mode="rw";pragma="&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=wal_autocheckpoint(1000)"}
	if len(offlineCacheMiB)>1{return nil,errors.New("invalid_offline_cache")};if len(offlineCacheMiB)==1{cache:=offlineCacheMiB[0];if cache<1||cache>1024{return nil,errors.New("invalid_offline_cache")};if apply{pragma+="&_pragma=cache_size(-"+strconv.Itoa(cache*1024)+")"}}
	dsn:=u.String()+"?mode="+mode+"&_pragma=busy_timeout(0)"+pragma
	w,err:=sql.Open("sqlite",dsn);if err!=nil{return nil,err};w.SetMaxOpenConns(1);w.SetMaxIdleConns(1)
	if err=w.PingContext(ctx);err!=nil{w.Close();return nil,err}
	r,err:=sql.Open("sqlite",ro);if err!=nil{w.Close();return nil,err};r.SetMaxOpenConns(1);r.SetMaxIdleConns(1)
	after,err:=os.Lstat(path);if err!=nil || !os.SameFile(before,after){w.Close();r.Close();return nil,errors.New("gc_database_replaced")}
	if !apply{return sqlitestore.NewReadOnlyStore(ctx,w,r)}
	return sqlitestore.NewStore(w,r),nil
}
