# Private, daemon-owned history maintenance

This Linux-only interface is optional and is **not** registered on the primary,
LAN, mobile, browser, or gateway HTTP router. No new network listener is added.
The normal daemon and persistent-host adoption continue if it is unavailable.

Enable only after review, with `ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET` pointing to
an operator-provisioned, same-UID, mode-0700 **local** directory. The socket is
mode 0600; each connection authenticates Linux `SO_PEERCRED` against the daemon
UID. Origin/Forwarded headers are refused. Symlink ancestors and network-mounted
paths are refused. Existing socket/file paths are preserved, never blindly
unlinked. Normal listener close removes its own socket; a stale path after an
unclean shutdown disables maintenance and emits a safe warning, not a fatal AO
startup error. The operator must prove a stale socket unused before retiring it.

Example existing user-unit addition (operator action, not automatically applied):

```ini
[Service]
EnvironmentFile=-/home/coland/.config/albanews-ao-history-retention.env
RuntimeDirectory=albanews-ao-maintenance
RuntimeDirectoryMode=0700
```

Set the private env's socket to
`/run/user/1000/albanews-ao-maintenance/ao.sock`. Keep the existing `KillMode=process`
and persistent host/native data intact. This change does not operate lifecycle,
resume, send, or provider processes.

## Protocol

- `GET /v1/capability`: schema readiness, protocol 1, PID/process start ticks,
  database device/inode, retention policy and configured write budget. No path,
  token, body, SQL, or native prompt is returned.
- `POST /v1/history/batch`: a typed `stage`, previous confirmed PK `cursor`, UTC
  `cutoff`, expected database device/inode, and optional lower limits
  (`batch_rows`, `batch_bytes`, `max_row_bytes`, `read_budget_ns`,
  `write_budget_ns`). Requests cannot raise daemon ceilings, select arbitrary
  tables/databases, provide predicates/SQL, or supply replacement text.
- Stages: `raw`, `outputs`, `message`, `activity`, `turn`, `session`, and bounded
  builtins `change_log`, `telemetry_event`, `notifications`,
  `agent_switch_failure_outbox`, `agent_switch_failure_receipts` (prefix `builtin:`).
- Responses carry only metadata, counts, safe reasons, cursor, and timing. The
  **private** cursor is necessary for resumption. `ao history-maintenance` prints
  only a fixed safe projection and cursor SHA, never the original cursor.

Only the compatible live daemon, exact sidecar schema, and supported policy
schema (191/192) authorize mutation. Public history-retention reader capability
alone is insufficient to enable this new writer path. A legacy source can still
be read/previewed without modification by the separate scheduler component.

## Coordination and proof

A separate non-queued single-maintenance admission bounds preparation memory before any DB query. Candidate PK/body reads, strict parsing, transformation, and full marker/fingerprint hashing finish before writer admission. Autocommit reader queries are closed before writeMu is requested; no read snapshot is held while waiting. Maintenance never queues on
`writeMu`: an active or already waiting business writer yields a deferred result.
After admission, pending business writers stop the batch at its next object
boundary. Each batch releases its single writer connection and token; the
scheduler yields **outside** that reservation. This is cooperative prioritization,
not a promise of zero business latency.

A maintenance-only pinned `sql.Conn` temporarily uses fail-fast SQLite busy
policy. Recovery is registered **before** its first PRAGMA mutation. The prior
busy policy is restored before returning the connection, or the connection is
explicitly discarded and reported. Cancellation/failed restoration never leaves
business writers with maintenance's busy policy. Business timeout/retry/ACK
settings are unchanged. Cleanup shares the original write deadline rather than
starting another wait window.

Inside the AO immediate transaction, only small headers/relations are fetched fresh. Exact original-value CAS binds the reader-prepared body (and its raw native proof) to the current record; identity/semantic/revision/branch differences are retained for re-preparation, not marked cleared. Eligibility is rechecked using the same transaction: age with nanoseconds, terminal state,
queued/running, pending input/approval, session/branch ownership, handoff and
closed-owner review protections. Unproven/malformed objects are retained.
No snapshot read or cached Python proof is treated as mutation authorization.
Content changes and immutable replay fingerprints/markers commit atomically;
message/activity revisions and original CDC triggers remain in force. IDs,
branches, client/provider facts, authentication facts and usage ledgers remain.
Ordinary event projection/ACK/drain/send paths are not changed.

A failed or canceled transaction does not advance the confirmed cursor. A
commit error or lost response is `unknown`, not a fabricated rollback or success;
retry uses the same PK prefix and marker/idempotence facts. Successful commits
remain counted if a subsequent connection restoration requires discard.

`held_ms` measures the SQLite transaction (including rollback). `writer_reserved_ms`
measures the complete AO writer reservation, including connection acquisition,
rollback, restoration/discard and close. `wait_ms`, `read_ms` (bounded preparation, including hashing),
`commit_ms`, maximums and defer counts are separate. JSON parsing cooperatively checks the reader deadline outside the business writer; canonical encoding and hashing also run there. Byte/row limits bound preparation. The writer does not reparse/canonicalize/hash bodies, only SQLite CAS comparisons and fresh small relationship checks. Actual elapsed
times remain evidence, not hard latency guarantees.

## Settings

The daemon and scheduler consume the same private `ALBANIA_AO_HISTORY_*` settings:
`RETENTION_HOURS` (47 by default, positive and at most 48), `BATCH_ROWS` (100),
`BATCH_BYTES` (16777216), `MAX_ROW_BYTES` (8388608), `STATEMENT_SECONDS` (2), and
`TRANSACTION_SECONDS` (0.25). Invalid/nonfinite/overflow settings disable only the
optional maintenance interface. CLI/request limits may lower these ceilings.
The scheduler separately controls total wall time, batch count and yielding.

Maintenance does not execute VACUUM, checkpoint, switch journal modes, replace a
live DB, clean native transcripts, or touch backups/NFS/cloud storage. Existing
marker growth and no-body audit rows are reported as costs. Logical body-byte
reduction, marker estimates, reusable pages, WAL and OS space are distinct.
A successful PK scan/pass does not prove the maximum-age objective was met.

Rollback: disable the scheduler first. The compatible marker readers remain
required after any projection redaction; never put an old incompatible binary
against the already-redacted live database, and never restore a whole old DB over
current host/queue facts. Root owns rollout, host/native/input proofs and any
separately controlled physical compaction.

Byte admission uses [SQLite octet_length(column)](https://www.sqlite.org/lang_corefunc.html#octet_length), which obtains TEXT/BLOB byte lengths from record metadata without reading the content. Maintenance requires UTF-8 so the budget and semantic UTF-8 hash units agree. Only fixed necessary stage/lifecycle columns are loaded; another object's unrelated large prompt/diff is not selected for qualification.

The effective maximum row bytes is clamped to the batch byte budget. An oversized first record is retained and reported rather than causing a permanent zero-progress prefix. Outputs use their own projection: unrelated activity summary/detail bodies are not selected or charged as output data. Builtin deletion preparation retains exact bounded original column bytes for CAS; fresh installed janitor predicates remain authoritative in the write transaction.
