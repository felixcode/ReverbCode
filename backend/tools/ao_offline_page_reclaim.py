#!/usr/bin/env python3
"""AO-owned native SQLite offline page reclamation; no row deletion or migration."""
import argparse
import contextlib
import fcntl
import json
import math
import os
from pathlib import Path
import sqlite3
import signal
import threading
import stat
import subprocess
import time


def identity(path):
    item=path.lstat()
    if not stat.S_ISREG(item.st_mode) or item.st_uid!=os.getuid() or stat.S_IMODE(item.st_mode)!=0o600 or item.st_nlink!=1:
        raise ValueError('unsafe_database')
    for parent in path.parents:
        if parent.is_symlink():raise ValueError('database_parent_symlink')
    return item


def schema(connection,allow_full_restore=False):
    version=connection.execute('SELECT max(version_id) FROM goose_db_version WHERE is_applied=1').fetchone()[0]
    auto=connection.execute('PRAGMA auto_vacuum').fetchone()[0]
    if version!=192 or auto not in ({1,2} if allow_full_restore else {2}):raise ValueError('requires_schema192_incremental')
    # Compile all schema table/index metadata using this exact native engine.
    tables=connection.execute("SELECT name FROM sqlite_schema WHERE type='table'").fetchall()
    for (name,) in tables:
        quoted='"'+name.replace('"','""')+'"'
        connection.execute('SELECT * FROM '+quoted+' LIMIT 0').close()
        connection.execute('PRAGMA table_xinfo('+quoted+')').fetchall()
        for index in connection.execute('PRAGMA index_list('+quoted+')').fetchall():
            connection.execute('PRAGMA index_xinfo("'+index[1].replace('"','""')+'")').fetchall()
    return dict(schema=version,auto_vacuum=auto,tables=len(tables),sqlite_version=sqlite3.sqlite_version)


def pages(connection):
    return dict(page_count=connection.execute('PRAGMA page_count').fetchone()[0],free_pages=connection.execute('PRAGMA freelist_count').fetchone()[0],page_size=connection.execute('PRAGMA page_size').fetchone()[0])


def reclaim_batch(connection,count,budget,cancelled=None):
    before=pages(connection);deadline=time.monotonic()+budget
    connection.set_progress_handler(lambda:int(time.monotonic()>=deadline or cancelled is not None and cancelled.is_set()),1000)
    commit_state='not_started';started=time.monotonic()
    try:
        if time.monotonic()>=deadline or cancelled is not None and cancelled.is_set():raise TimeoutError('cancelled_or_deadline')
        connection.execute('BEGIN IMMEDIATE');commit_state='open'
        connection.execute('PRAGMA incremental_vacuum('+str(min(count,before['free_pages']))+')').fetchall()
        if time.monotonic()>=deadline or cancelled is not None and cancelled.is_set():raise TimeoutError('cancelled_or_deadline')
        after=pages(connection)
        if after['free_pages']>before['free_pages'] or before['free_pages']-after['free_pages']>count or after['page_count']>before['page_count']:
            raise ValueError('invalid_page_delta')
        commit_state='unknown';connection.commit();commit_state='committed'
        return dict(before=before,after=after,free_pages_removed=before['free_pages']-after['free_pages'],logical_bytes_removed=(before['page_count']-after['page_count'])*before['page_size'],elapsed_ms=int((time.monotonic()-started)*1000),commit_state=commit_state)
    except BaseException as error:
        connection.set_progress_handler(None,0)
        if connection.in_transaction:
            try:
                connection.rollback()
                if commit_state=="open":commit_state="rolled_back"
            except sqlite3.Error:commit_state="unknown"
        error.ao_commit_state=commit_state
        error.ao_error_enum="context_canceled" if cancelled is not None and cancelled.is_set() else "deadline_exceeded" if isinstance(error,TimeoutError) or time.monotonic()>=deadline else "sqlite_error"
        raise
    finally:connection.set_progress_handler(None,0)


def restore_incremental(connection):
    # SetAutoVacuum changes the connection flag before writing meta[6]. This
    # separate statement therefore cannot itself trigger another FULL reclaim.
    if connection.in_transaction:connection.rollback()
    connection.set_progress_handler(None,0)
    connection.execute('PRAGMA auto_vacuum=INCREMENTAL').fetchall()
    if connection.execute('PRAGMA auto_vacuum').fetchone()[0]!=2:
        raise ValueError('incremental_restore_failed')


def full_reclaim_once(connection):
    before=pages(connection);started=time.monotonic();state='not_started'
    connection.set_progress_handler(None,0)
    try:
        connection.execute('BEGIN IMMEDIATE');state='open'
        connection.execute('PRAGMA auto_vacuum=FULL').fetchall()
        # SQLite 3.46 autoVacuumCommit's bCommit=1 path clears the freelist
        # once, skipping per-free-page BTALLOC_EXACT. Its inner commit loop
        # does not provide an interrupt/progress deadline; never hard-kill it.
        state='unknown';connection.commit();state='committed'
        after=pages(connection)
        if after['free_pages']!=0 or after['page_count']>before['page_count']:
            raise ValueError('full_reclaim_invalid_delta')
        return dict(before=before,after=after,free_pages_removed=before['free_pages'],logical_bytes_removed=(before['page_count']-after['page_count'])*before['page_size'],elapsed_ms=int((time.monotonic()-started)*1000),commit_state=state,full_commit_noninterruptible=True)
    except BaseException as error:
        if connection.in_transaction:
            try:
                connection.rollback()
                if state=='open':state='rolled_back'
            except sqlite3.Error:state='unknown'
        error.ao_commit_state=state
        raise
    finally:
        # Restoring 2 is a distinct durable transaction, including after a
        # failed/rolled-back FULL attempt; in-memory flags alone are insufficient.
        try:restore_incremental(connection)
        except BaseException as error:
            error.ao_commit_state=state;error.ao_phase="restore_incremental";raise


def stopped():
    for command in [['systemctl','show','albanews-ao-history-retention.timer','--property=ActiveState','--value'],['systemctl','--user','show','albanews-ao-component-user.service','--property=ActiveState','--value'],['systemctl','show','albanews-ao-history-retention.service','--property=ActiveState','--value']]:
        value=subprocess.run(command,capture_output=True,text=True,check=True).stdout.strip()
        if value not in {'inactive','failed'}:raise ValueError('ao_or_maintenance_active')


def save(path,value):
    temporary=path.with_name(path.name+'.new')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        data=(json.dumps(value,indent=2)+'\n').encode()
        with os.fdopen(fd,'wb') as stream:stream.write(data);stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,path)
    except BaseException:
        raise


def run():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',type=Path,required=True);p.add_argument('--database-device',type=int,required=True);p.add_argument('--database-inode',type=int,required=True)
    p.add_argument('--maintenance-lock',type=Path,required=True);p.add_argument('--report',type=Path,required=True)
    p.add_argument('--full-once',action='store_true',help='one offline FULL autovacuum commit then restore INCREMENTAL; commit has no hard deadline');p.add_argument('--restore-incremental-only',action='store_true',help='recover mode2 and checkpoint after an interrupted full-once');p.add_argument('--max-batches',type=int,default=0);p.add_argument('--inspect-only',action='store_true');p.add_argument('--batch-pages',type=int,default=4096);p.add_argument('--batch-seconds',type=float,default=30);p.add_argument('--wall-seconds',type=float,default=3600);p.add_argument('--cache-mib',type=int,default=128)
    a=p.parse_args();os.umask(0o077)
    if not a.database.is_absolute() or min(a.batch_pages,a.batch_seconds,a.wall_seconds,a.cache_mib)<=0 or a.cache_mib>1024 or a.max_batches<0 or a.full_once and a.restore_incremental_only or a.inspect_only and (a.full_once or a.restore_incremental_only) or not all(math.isfinite(v) for v in (a.batch_seconds,a.wall_seconds)):raise ValueError('invalid_options')
    parent=a.report.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid!=os.getuid() or stat.S_IMODE(parent.st_mode)!=0o700 or a.report.parent.is_symlink():raise ValueError('unsafe_report_parent')
    before=identity(a.database)
    if (before.st_dev,before.st_ino)!=(a.database_device,a.database_inode):raise ValueError('database_identity_changed')
    stopped();lock_item=identity(a.maintenance_lock)
    with a.maintenance_lock.open('r+') as lock:
        if not os.path.samestat(lock_item,os.fstat(lock.fileno())):raise ValueError('lock_identity_changed')
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        paths=[str(a.database)]+[str(a.database)+suffix for suffix in ('-wal','-shm','-journal') if Path(str(a.database)+suffix).exists()]
        probe=subprocess.run(['sudo','-n','fuser','-s',*paths],capture_output=True)
        if probe.returncode!=1 or probe.stderr:raise ValueError('database_references_not_closed')
        uri=a.database.as_uri()+'?mode='+('ro' if a.inspect_only else 'rw')
        with contextlib.closing(sqlite3.connect(uri,uri=True,isolation_level=None,timeout=0)) as connection:
            connection.execute('PRAGMA busy_timeout=0')
            if a.inspect_only:connection.execute('PRAGMA query_only=ON')
            qualification=schema(connection,a.restore_incremental_only)
            if not os.path.samestat(before,identity(a.database)):raise ValueError('database_identity_changed')
            result=dict(status='running',stage='native_page_reclaim',qualification=qualification,full_once=a.full_once,restore_incremental_only=a.restore_incremental_only,full_commit_hard_deadline=False if a.full_once else None,committed_batches=0,free_pages_removed=0,logical_bytes_removed=0,main_file_bytes_before=before.st_size,database_device=before.st_dev,database_inode=before.st_ino)
            if not a.inspect_only:
                connection.execute('PRAGMA synchronous=FULL');connection.execute('PRAGMA cache_size=-'+str(a.cache_mib*1024));connection.execute('PRAGMA wal_autocheckpoint=0')
            deadline=time.monotonic()+a.wall_seconds
            cancelled=threading.Event()
            def request_stop(*_):
                cancelled.set();connection.interrupt()
            signal.signal(signal.SIGTERM,request_stop);signal.signal(signal.SIGINT,request_stop)
            save(a.report,result)
            phase='native_reclaim'
            try:
                if a.restore_incremental_only:
                    phase='restore_incremental';restore_incremental(connection)
                    phase='native_checkpoint';checkpoint=connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
                    if checkpoint[0]!=0:raise ValueError('checkpoint_busy')
                while not a.inspect_only and not a.restore_incremental_only and pages(connection)['free_pages']>0:
                    if time.monotonic()>=deadline or cancelled.is_set():result['status']='partial';break
                    phase='native_full_commit' if a.full_once else 'native_reclaim'
                    result['stage']=phase;save(a.report,result)
                    last=full_reclaim_once(connection) if a.full_once else reclaim_batch(connection,a.batch_pages,min(a.batch_seconds,deadline-time.monotonic()),cancelled)
                    result['last_batch']=last;result['committed_batches']+=1;result['free_pages_removed']+=last['free_pages_removed'];result['logical_bytes_removed']+=last['logical_bytes_removed']
                    phase='native_checkpoint'
                    connection.set_progress_handler(lambda:int(cancelled.is_set() or time.monotonic()>=deadline),1000)
                    try:checkpoint=connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
                    finally:connection.set_progress_handler(None,0)
                    if checkpoint[0]!=0:raise ValueError('checkpoint_busy')
                    result.update(main_file_bytes=a.database.stat().st_size,at=time.time(),filesystem_available_bytes=os.statvfs(a.database.parent).f_bavail*os.statvfs(a.database.parent).f_frsize)
                    save(a.report,result)
                    phase='native_reclaim'
                    if last['free_pages_removed']==0:raise ValueError('no_reclaim_progress')
                    if a.full_once:break
                    if a.max_batches and result['committed_batches']>=a.max_batches:
                        result['status']='partial';break
                if result['status']=='running':result['status']='preview_complete' if a.inspect_only else 'complete'
            except BaseException as error:
                result['status']='failed';result['failed_phase']=getattr(error,'ao_phase',phase);result['failed_batch_commit_state']='committed' if phase=='native_checkpoint' else getattr(error,'ao_commit_state','unknown');result['error_type']=getattr(error,'ao_error_enum',None) or ('interrupted' if isinstance(error,sqlite3.OperationalError) and getattr(error,'sqlite_errorcode',None)==sqlite3.SQLITE_INTERRUPT else 'sqlite_error' if isinstance(error,sqlite3.Error) else 'guard_or_io_error')
                if isinstance(error,sqlite3.Error):result['sqlite_code']=getattr(error,'sqlite_errorcode',None)
                save(a.report,result);raise
            result['final_auto_vacuum']=connection.execute('PRAGMA auto_vacuum').fetchone()[0]
            if result['final_auto_vacuum']!=2:raise ValueError('incremental_restore_failed')
            result['final_pages']=pages(connection);result['main_file_bytes']=a.database.stat().st_size;save(a.report,result)
            print(json.dumps({k:result[k] for k in ('status','committed_batches','free_pages_removed','logical_bytes_removed','main_file_bytes')}))
    return 0


if __name__=='__main__':
    try:raise SystemExit(run())
    except Exception as error:
        print(json.dumps(dict(status='failed',error_type='sqlite_error' if isinstance(error,sqlite3.Error) else 'guard_or_io_error',sqlite_code=getattr(error,'sqlite_errorcode',None))))
        raise SystemExit(1)
