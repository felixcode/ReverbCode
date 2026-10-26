# SQLite writer transaction contention

The writable store uses one write connection and a separate eight-connection
reader pool. `Open` and the test-template `OpenPreMigrated` append
`_txlock=immediate` only when opening the writer. Reader DSNs and `OpenReadOnly`
remain unchanged. URI escaping, WAL, synchronous mode, foreign keys and the
existing 5000 ms busy timeout are unchanged.

The pinned modernc SQLite v1.51.0 implementation accepts `deferred`, `immediate`
and `exclusive` transaction modes. Its `newTx` applies the selected mode only to
transactions without `ReadOnly`. Acquiring the WAL writer before reading current
owner/retention evidence avoids a deferred snapshot having to upgrade after a
competing writer commits. It does not eliminate ordinary contention or prove that
a historical primary SQLITE_BUSY was specifically BUSY_SNAPSHOT. Other processes
can still write the file; an unavailable writer remains a real bounded failure.

Provider archive, owner/generation/branch checks, expired-content guards and
projection still commit or roll back atomically. Existing projection attempts,
backoff, acknowledgements and retained-queue behavior are unchanged. No automatic
prompt resend, replay bypass, new daemon or timeout increase is introduced.

Only the existing persistent-projection failure log gains `phase`, `attempts`,
`duration_ms` and available numeric SQLite primary/extended codes. ACK failures
are marked `ack`; unknown codes are omitted, not reported as SQLite success.
Operational metadata adds no SQL, DSN, prompt, event body or credential fields.

Targeted synthetic tests exercise deferred upgrade failure as a control, both
writer-reservation orders, parallel WAL readers, cancellation/rollback and exact
retry identity. Relevant existing archive/ACK, generation/authentication,
live-transport and preserved-queue guards remain required. All fixtures are
private temporary databases; no production database is copied or modified.
