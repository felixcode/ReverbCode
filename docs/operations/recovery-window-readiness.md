# Isolated database recovery readiness (2026-10-27)

This is a fixture-only preparation package, not a shutdown, database replacement,
native reconnect or provider authorization. `test/maintenance/recovery-readiness.sh`
runs a fixed targeted selection and accepts only a new output directory. It has
no database, host, arbitrary command, resume, Send, Retry or candidate-install
argument. Run it in a disposable Linux container with no network, readonly source
code, private HOME/tmp/output, no AO state/host/socket directories, no provider
executables or credentials, and bounded CPU/memory. The tested RC configuration
uses 1 CPU/2 GiB. Ordinary HTTP checks address only the child daemon's private
loopback endpoint and verify its PID; no AO message endpoint is invoked.

The script builds the exact checkout and verifies:

- A tiny full application schema survives the reviewed ROWID-preserving rebuild,
  reopens through `OpenPreMigrated`, changes database inode identity, and preserves
  its complete queued input projection. Retrying the original client input ID
  creates no second input/turn; changed payload under that ID remains a conflict.
- An **empty synthetic rebuilt database** boots the real `ao daemon`, answers
  health/ready with the child PID, shuts down gracefully and still has no sessions.
  `ao start` is not used: it launches/downloads the desktop application.
- Synthetic ACP SDK LoadSession fully drains 4096 question/answer exchanges
  (about 20 MiB), repeats with stable identities, handles detached replay with a
  slow consumer, and retains the original terminal turn identity.
- Codex app-server scripted pipe responses exercise native thread history,
  restart/replay identity, absent-host refusal, and original input idempotency.
  Durable archive/generation checks, controller recovery and startup reconciliation
  use their existing focused tests. No test launches a real native provider.

The existing 65536 notifications/128 MiB ACP configuration and 5-minute request
timeout are used unchanged. The 2026-10-26 independent evidence measured a
particular 85 MB ACP prompt burst with 46453 queued notifications; it is not a
current full-native-history guarantee. Four 2026-10-27 Codex rollout file prefixes
totalled 816607143 bytes, maximum complete file record 7594732 bytes. Neither
these file sizes, `acp-prompt.journal`, nor the 20 MiB fixture establish the current
wire replay volume, protocol frame distribution, memory cost or replay duration
for every supervisor/worker. No limits are raised to hide this unknown.

The **actual rebuilt candidate boot**, complete current native replay, exact
provider/version/profile compatibility, and recovery after the future real
transfer remain unverified until their inputs exist. A historical candidate
cannot replace current production. Fixture success cannot prove real native
process recovery, delivery or a safe shutdown window.

## Future planned window order

These are preparation instructions for root and the Native supervisor. They are
not executed by this package and create no scheduler, service or backup task.

1. Schedule the window in AO. Drain work, retain original pending input IDs,
   payload hashes, provider/native IDs, controller generation and persistent-host
   PID birth identity. Preserve required native state/journals. Confirm fresh
   complete replay evidence for each affected native session under unchanged
   budgets; changed inputs require new evidence. File-prefix counts are insufficient.
2. Pause the **system** history-retention service/timer, gracefully stop the
   **user** AO daemon using its supported procedure, and prove no SQLite writer
   remains. Preserve idle persistent hosts; do not kill provider processes as a
   substitute for draining or edit their journals/ACKs.
3. Inspect the closed main/WAL/SHM group. A normal graceful stop may leave an
   independently consistent main file, but this must be proved. If committed WAL
   frames remain, copying only main is unsafe: preserve the group and use the
   reviewed supported consistent-snapshot/recovery branch. Never hand-delete WAL
   or perform an unplanned checkpoint. This preparation performs neither.
4. Stream the **fresh consistent** source to remote storage and independently
   verify its recovery path, SHA and required WAL/config metadata before retiring
   the local old source. Recovery material must correspond to this exact stopped
   source, not a historical experiment. Measure transfer/restore duration.
5. Run accepted cleanup/rebuild against that fresh source remotely; verify schema,
   typed values, IDs/ROWIDs, FKs/integrity and pending original-input continuity.
   Validate an unpublished isolated candidate with no live host/credentials/network;
   this still cannot prove reconnect to the real surviving native hosts.
6. With the old local DB/WAL/SHM preserved remotely and all writer references closed,
   retire the old local group before streaming the new local database. Avoid old
   DB plus full `new.tmp` locally. Do not reuse an unrelated old WAL/SHM. A failed
   transfer now requires remote restore and counts toward downtime; this is not
   an instantaneous atomic switch. Verify file/parent-directory fsync, permissions,
   exact SHA and new inode before startup. Future transfer/restore times remain U.
7. Start the accepted runtime with the new local DB identity; reset maintenance
   cursors/receipts to that identity. Before **any possible new writes**, rollback
   may restore the independently verified matching recovery point. After possible
   new writes, preserve the new group/facts first; never blindly overwrite it with
   the old full database or resend uncertain inputs with new IDs. Incompatible old
   runtime maintenance stays disabled on rollback.
8. Verify actual Native supervisor/worker reconnect and one authorized continuation
   with original input/provider-turn identities. Process survival or health alone
   is insufficient. Resume maintenance only against the new identity and supported
   capability. Observe body/metadata/free pages/DB/WAL blocks separately, including
   the planned 72h and two 01:00 reports. Body cleanup and page reclamation do not
   prove metadata growth or repeated scanning is permanently bounded.
