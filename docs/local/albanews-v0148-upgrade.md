# Albanews AO v0.14.8 preparation (2026-11-02)

This tree starts at official `v0.14.8` (`403a8c8385ed43bbf9e9289e3f3bbaa6517dc437`) and carries the necessary Albanews runtime changes from `38951fb437e60877e56ee5d1117b4237f3d3571a`. It is a locally patched build. Preparation does not authorize stopping the supervisor, workers or AO, replacing the running binary, pausing timers, or migrating the production database. Those actions wait for the user's downtime instruction and root's independently reviewed execution package.

The retained changes include the private Unix maintenance writer, retention markers and late-event suppression, closed raw/activity cleanup, bounded live-turn/authentication reads, durable provider event ACK and backpressure, queue recovery, provider profile pinning, query indexes and UI expiry projections. Upstream's new provider-switch implementation remains the active implementation. The obsolete old AgentSwitch executors are not restored. Legacy handoff lookup and unresolved-switch recovery/retention references remain read-only; unresolved legacy switches or the latest recovery-required interface transition refuse new provider-switch admission.

## Database compatibility

The local fork used Goose IDs 191 and 192 for provider-context columns and a running-review index. Official 191 adds session branch state and its CDC trigger; official 192 extends the harness constraint. The original official migration files remain intact. `prepareAlbanews0148Bridge` runs before Goose 193+, recognizes the known old schema, applies the complete official 191/192 Up bodies without editing their existing ledger rows, preserves session rowids and local columns, and permits a completed-191 retry. A branch column without its corresponding CDC trigger or an unknown collision shape is rejected. Fresh databases follow the official migration sequence, then receive the small local provider columns/indexes.

The maintenance schema policy explicitly recognizes the reviewed official maximum migration, retaining refusal for unknown versions. New page-walk and human-prompt snapshot projections use the existing snapshot transaction; they do not obtain another pool connection while that transaction is held.

Official migration `20261024194546592` builds/backfills message FTS5; `20261028124744441` enables incremental auto-vacuum and performs an initial full `VACUUM` on the main `ao.db`. These are not skipped or marked applied early. Their duration, temporary space use and production-size growth remain unmeasured until an authorized isolated copy is available. Synthetic migration tests use the actual official modernc SQLite driver, including FTS and VACUUM.

## Storage-only helper

Build `backend/cmd/ao-upgrade-schema` from the same tree and SQLite configuration as the server:

```sh
cd backend
go build -o /private/output/ao-upgrade-schema ./cmd/ao-upgrade-schema
/private/output/ao-upgrade-schema --data-dir /literal/isolated-copy-directory
```

The directory must be absolute, canonical and already contain `ao.db`. The helper only opens/migrates/closes storage; it never starts a daemon, reconciler, controller or provider. It refuses the known production namespace. The operator wrapper additionally binds scratch/source identities and creates the rollback snapshot. No production-copy operation has been performed during preparation.

Success is JSON `status=ready`, `daemon_started=false`, `provider_started=false`. Failure exposes only controlled JSON fields: `stage` (`input_validation`, `storage_migration`, `storage_close`) and fixed `error_code`. Migration failures may add `migration_stage` (`bridge`, `official_goose`, `local_provider_schema`) and `migration_version` as a canonical decimal string drawn only from embedded official migration filenames. Unknown version is omitted. SQL, raw driver errors, paths, credentials and payloads are never emitted by the helper.

The preparation operator is external evidence at `logs/diagnose/ao-upgrade-20261102/audit/isolated-db-validation.py` in the Albanews repository. Its copy/migrate/verify/verify-restored modes apply only to explicit isolated copies. Rolling back an upgraded database means restoring the same fresh snapshot, not running Goose Down. A binary-only rollback must not be assumed safe for the migrated database or newly accepted inputs.

## Runtime and build settings

Use the official Go toolchain requirement (validated here with Go 1.27.1), `GOAMD64=v1`, no extra build tags, and the official `modernc.org/sqlite v1.51.0` driver. The preparation build declares `CGO_ENABLED=1`, as the prior local build did; SQLite itself is the official pure-Go implementation. Server and helper use identical capabilities. Inject CLI version `v0.14.8+albanews` and the exact built source commit through the existing official `internal/cli.Version`, `Commit`, and `Date` linker variables.

The candidate-only environment delta sets `AO_TELEMETRY_RETENTION=48h` to preserve the user's two-day limit; the official default is 30 days. It has not changed the live environment. The official telemetry sink receives the same Store as owned maintenance. Telemetry create/prune and incremental reclaim use that Store's existing single writer pool, not another external SQLite writer. Telemetry pruning owns telemetry rows; the private Unix history maintenance path owns its separate history stages.

Every isolated CLI/test invocation removes inherited `AO_*` worker/runtime variables and uses its own `AO_DATA_DIR`, ports and synthetic provider. The compatibility smoke builds the exact old persistent-host source and attaches the new consumer to that host, checking original event identities and durable projection across detach/reconnect. No real AO conversation or model receives a prompt. Protocol version 1 and unchanged PIDs alone are not compatibility evidence.

## Evidence and pending operations

SQLC, API YAML and TypeScript API schema are regenerated using the repository-pinned generators. Targeted migration, provider-scope, transport, queue/ACK and retention guards plus one combined isolated smoke are recorded outside Git under `logs/diagnose/ao-upgrade-20261102`. Passed unchanged tests are reused; no full suite is run.

Root receives the exact source/transport binding, per-file patch inventory, generated-source hashes, binary digest, build settings and independent review. Actual current-database migration time/space, a fresh cutover rollback point, real supervisor handoff and production post-cutover behavior remain unverified preparation items. The existing current binary/source remain the rollback reference. Production actions pause until the user gives the downtime instruction.


## 2026-11-02 closed-owner environment unit correction

The inherited `StartConfigured` hours classification omitted
`CLOSED_OWNER_RETENTION_HOURS`, so an explicit value24 became24 seconds.
The corrected classification reads all three declared hour settings as hours;
statement and transaction budgets remain seconds. User values12h/6h/24h stay
unchanged. Focused tests exercise the actual environment entry, private Unix
capability and HTTP budgets with small synthetic metadata only. The successor
rebuilds the server; the already-validated schema and storage helper are reused.
