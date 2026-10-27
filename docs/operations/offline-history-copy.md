# Offline history copy cleanup rehearsal (2026-10-27)

This separate Linux program prepares and cleans **an exclusively new private
working copy**. It never starts AO, opens its original source for writing,
publishes a candidate, replaces a database, or changes any live maintenance
setting. Deploying this source and changing production remain separate actions.

## Source, isolation and fixed time

`ao-history-copy-cleanup` requires a sealed private source, its exact SHA256,
a source creation `--as-of` timestamp, and an absent working directory.
The SHA is checked by the existing streaming copy before SQLite opens the new
copy or folds audit data. Existing rawcopy ownership, canonical path, local
filesystem, sidecar, source stability, O_EXCL/new-inode and process-reference
checks remain in force. An unknown process reference refuses clean-copy.
No existing-target, resume, force, arbitrary SQL or inplace option exists.

A private PID namespace is **insufficient** by itself. The reviewed executor
must first establish a new empty Docker-private volume, prove no other running
or stopped container references it, mount only that volume writable, mount only
the sealed source file readonly, use a readonly container root, disable network,
drop capabilities and run uid1000. The working inode is created inside that
previously empty volume; no AO data/config/socket directory is mounted, no
application registers or publishes it, and the executor preserves before/after
volume and inode evidence. The post-run stat/copy must happen after the tool
exits. Host application data is never made accessible through this interface.
Unknowns are retained/refused, never hidden by adding an offline flag.

Runtime databases stay on bender. The real rehearsal is intended for jimmy's
server-local path, not an SQLite connection over NFS and not an additional full
copy on bender. The original source and producer recovery-point TTL are unchanged.
The historical source uses `2026-10-25T21:28:16.835298Z` and 47h retention;
this is a historical qualification experiment, not a current retention claim.

## Application policy and bounded execution

The first phase reuses the reviewed rawcopy fold. Then the exact new fixed file
`raw-audit-working.sqlite3` is opened with separate writer/readonly pools and
`NewStore`, with application foreign-key enforcement preserved; no migration or `EnsureSchema` path runs. Each batch calls the same
application preparation, eligibility, original-value CAS, marker, writer admission,
commit and ACK-related rules as the daemon. No simplified deletion predicates
are introduced. The only store refactor passes a historical clock through the
existing method body. The live wrapper still uses `time.Now()` and existing
limits; no HTTP field or endpoint is added. Future/zero historical clocks fail.

Stage order is raw, outputs, message, activity, turn, session, change_log,
telemetry_event, notifications, agent_switch_failure_outbox and
agent_switch_failure_receipts. The final five use their installed application
janitor predicates, with the fixed historical clock where applicable. Failed or
unknown commits stop without advancing a confirmed cursor or inventing counts.
A wall/batch cap or zero confirmed progress is partial, not complete.
Before each stage, a readonly check reuses its existing table/PK metadata to
count unsupported keys: nonpositive/NULL/noninteger numeric keys, and
empty/NULL/nontext TEXT keys. The cursor lower-bound enum and count are reported;
any unsupported prefix leaves complete=false. This verifies selector coverage,
not eligibility: it does not compare a filtered candidate count with all table
rows or expand deletion rules.

All copy/batch/read/write/fold/wall/progress budgets are flags. Initial default
wall budget is two hours; the executor must allow extra outer termination/report
headroom, and root chooses the real measured budget. Progress is a small closed
phase/count/time JSON on stderr (phase changes immediately, repeated phases at
the configured interval). It includes no raw cursor, body, path or credential.
Stdout and `history-copy-report.json` contain the final safe report.

## Reports and rebuild handoff

The source and cleaned copy have one row-count/primary-key census per table.
TEXT/composite primary keys retain an additional digest of their implicit ROWID
mapping. INTEGER PRIMARY KEY alias detection checks for a separate PK index
(the DESC exception); WITHOUT ROWID and no-key/virtual/unknown cases are explicit.
Only type-tagged, JSON-framed key tuples enter SHA256; no row values are output. Invalid UTF8 text keys refuse the identity proof rather than being silently normalized by JSON.
The cleaned identity census is the baseline for a later rebuild comparison.
Census reads do not expand arbitrary body columns. Each physical census walks
[dbstat aggregate mode](https://www.sqlite.org/dbstat.html) once (one output row per B-tree, avoiding a multi-million-row external GROUP BY), reporting per-table/index page allocation, payload and in-page
unused bytes. Missing dbstat is an explicit unknown. Freelist pages, DB/WAL file
lengths and logical byte changes remain separate units.

Application `logical_bytes_delta` (reported here as `net_body_column_bytes_removed`) is old column bytes minus new column bytes:
positive means net column reduction, and raw includes its new inline audit.
It does not subtract added sidecar/index costs. Raw fold's signed logical delta
uses the opposite direction: positive adds record data, negative reduces it.
Neither value is OS space released. Last connection close may normally checkpoint
this private copy; final physical lengths are recorded after close, with no
explicit checkpoint statement. Allocation/identity work shares the original wall
budget; missing post-census evidence remains incomplete.

Most content stages preserve event/raw/message/activity/turn/session rows and IDs.
All eleven scans completing **does not bound their row counts**, prove a 48h age
target, or solve future metadata growth. Retained reason counts include age,
active/protected and unknown cases; they are not all removable backlog. Sustainable
daily capacity and any application-owned retirement policy need separate measured
row/byte trends. No additional DELETE is hidden in this package.

No VACUUM, optimize, ANALYZE, journal-mode change or rebuild is performed. AO uses
implicit conversation_turns ROWID for rollback cuts and transition ROWID for
ordering. A naive VACUUM INTO is unsafe; a later reviewed rebuild must preserve
business identities, ROWID mappings, schema/FKs and necessary evidence. The new
candidate is not promoted by this rehearsal.

## Commands

Default preview creates a different new copy and inventories it; it does not
run the full application cleanup and always leaves `complete=false`.
Use `--clean-copy` for the reviewed isolated rehearsal:

```sh
/tool/ao-history-copy-cleanup \
  --snapshot /snapshot/sealed.sqlite3 \
  --expected-source-sha256 <source-manifest-sha256> \
  --new-working-directory /work/<new-nonce> \
  --as-of 2026-10-25T21:28:16.835298Z --retention 47h \
  --clean-copy --wall-budget 2h --max-batches 100000 \
  --batch-rows 100 --read-budget 2s --write-budget 250ms \
  --fold-budget 1s --progress-interval 30s
```

Do not run this example against a current AO path. Failed/partial working files
remain private for owner inspection; this program performs no automatic deletion.
Cleanup, candidate handoff, remote expiry and production shutdown are owned by
the external reviewed executor and root.
