# Raw history audit minimization (2026-10-27)

This candidate is based on deployed `fix/reconnect-replay-backpressure`
`b229f45d412e56f5eb4ae0e25654853677ff562f`, not upstream `main`, which does not
contain the locally deployed retention/reconnect fixes. The operator explicitly
requested this deployment branch and targeted tests, guards and one synthetic
smoke. No production migration, restart, cleanup, or claimed disk recovery is
part of this source delivery.

## Consumer census and unchanged boundaries

| Consumer | Proof / retained contract |
| --- | --- |
| `service/chat/controller.go`, `projectAndApply` | Archives an explicit top-level map. Provider detail/input is nested; provider content cannot inject a top-level `_retention` key. Normal event projection never creates a full audit. |
| `store/conversation_store.go`, `projectProviderEventTx`; `queries/conversations.sql`, `InsertConversationProviderEvent`; migrations 0066/0087/0149 | Raw is `INSERT OR IGNORE`; exact nonempty `(conversation_id,provider_event_id)` is unique. An existing identity never replaces payload. Empty IDs still append and newly reconciled IDs still use ordinary projection. Branch insertion triggers only assign branch metadata. |
| `store/native_history_archive.go` | Cold import tests exact archived identity, not raw sidecar rows or body hashes. |
| `store/history_retention.go`; generated message/activity/turn/session queries | Late-event redaction, replay fingerprints/counts, retry/edit guards and display use message/activity/turn/session/coordination markers. **All these kinds remain unchanged.** Short late-event `{protocol:1,contentExpired:true}` is not complete audit evidence. |
| `store/preserved_queue_recovery.go` | Reads raw method/payload authorization headers, not `provider_event` sidecars. Native IDs, error nullness, usage, unknown headers, raw rows and IDs remain. |
| `store/history_maintenance*.go` | Raw sidecar had been used to avoid repeated audit. New owned preparation also recognizes strictly bound inline audit, with the same body-size/read snapshot, exact original-body CAS and metadata comparison before advancing. |
| albanews `tools/ao_history_retention_core.py` | Legacy preview only; apply refuses external SQLite writing. Its raw parser reads top-level headers and tolerates the optional inline audit map. Old maintenance binaries must remain disabled after rollback, as they may rebuild duplicate raw sidecars. |
| albanews `tools/ao_history_retention_backlog.py` | Read-only age/length estimate uses sidecars. It may overestimate inline-only raw bodies/unmarked rows. Treat as an upper bound, never freshness/completion evidence. Active daemon-owned apply does not call this scanner. No Python change is included. |

## Two digest domains, one complete audit

The existing inline `original_sha256` hashes original `payload_json` UTF-8 bytes.
The old sidecar `original_sha256` hashes canonical JSON of
`{"payload_json": original_payload_string}`. They intentionally differ and are
never compared as equal. New inline `_retention.field_map_audit` preserves:

- `object_kind = provider_event`, exact canonical integer `object_id`, and
  `conversation_id` binding;
- `field_map_original_sha256`, the second canonical-map digest.

The existing protocol, timestamp strings, fields, original byte count and raw
payload digest are unchanged. Inline field lists are nonempty/unique, only use
recognized body keys and describe empty values. Remaining recognized body
content, invalid UTF-8/JSON, duplicate keys, malformed timestamps/digests,
foreign/incomplete/unknown audit extensions are not treated as expired proof.
Ordinary body-less raw events are checked but unchanged: no new marker, no false
claim that an expiration fingerprint was persisted. Unsupported body types are
retained. A short late-event inline marker remains unproven for this audit fold.

A historical fold requires the sidecar's exact ID/owner, empty semantic/prompt
hashes, original byte count, timestamp strings and canonical field list to agree
with its own complete inline audit. Any pre-existing binding must also agree.
The second digest is transferred before deleting the sidecar in the **same
transaction**. The already-removed original body cannot be rehashed; agreement
of the retained audit facts is not a claim of recovering or recomputing it.
A single sidecar with no full inline audit is retained. Other marker kinds,
orphan/mismatched/foreign/unknown facts are retained. Source payload and all audit
columns are CAS compared before update/delete; errors roll back the whole batch.

## Copy-only migration tool

`go build ./cmd/ao-raw-audit-copy` produces a separate Linux tool, **not an AO
CLI/live maintenance endpoint**. It never writes its source. It creates the
working directory itself (must not exist), a private new 0600 inode named
`raw-audit-working.sqlite3`, and a 0600 safe report. It rejects `ao.db` as source,
source WAL/SHM/journal, symlinks, hardlinks, nonlocal/unknown filesystems, wrong
ownership/mode, changed source identity/mtime/ctime/length, and input/copy budgets.
Only a sealed local snapshot is accepted; do not copy a live main SQLite file.
`quick_check(1)` runs only against the copy. No migrations, journal-mode change,
checkpoint or VACUUM is performed. Existing copy paths cannot be reopened for
apply; rerun creates another new copy from the sealed source.

Apply additionally checks other local `/proc` FD/mapping metadata for the new
copy identity; unreadable processes or possible references refuse apply. No
cmdline/environment/payload is read. The conservative inode check may reject
unrelated inode collisions. Run only where full local reference visibility is
available; do not bypass unknowns. The new private directory/copy is never
registered as an AO data directory. This limited interface does **not** prove an
arbitrary production path offline or authorize live external writing. A later
planned-stop application executor is required for any production integration.

Example on a disposable synthetic or explicitly sealed private snapshot only:

```sh
# Default is preview; both invocations require a different, absent directory.
ao-raw-audit-copy --snapshot /tmp/private-example/sealed.sqlite3 \
  --new-working-directory /tmp/private-example/preview \
  --max-copy-bytes 104857600 --batch-rows 100 --max-batches 10 \
  --max-row-bytes 8388608 --batch-bytes 16777216 \
  --wall-budget 1m --batch-budget 250ms
# Apply performs the same copy/identity/reference checks, then only folds that copy.
ao-raw-audit-copy --snapshot /tmp/private-example/sealed.sqlite3 \
  --new-working-directory /tmp/private-example/modified --apply
```

All operational copy/batch limits are flags; no production configuration changes
are implied. stdout is only fixed counts/digests/status, with no raw cursor/ID,
body, key or path. Failure exits nonzero and leaves only this task's private
working copy for explicit owner inspection/retirement. No auto-delete or force
option exists. Preview and apply have finite batch/wall budgets and may be
partial. A failed commit is not reported as a confirmed migration. No current DB
replacement, file publication, upload or daemon action exists in this tool.

## Cost, deployment and rollback

Report separately: logical body removal; added inline audit bytes; removed
sidecar logical bytes; SQLite table/index allocation, reusable pages, DB/WAL and
OS file bytes. `logical_bytes_delta` is signed: positive means added record data;
it excludes b-tree/record framing and is **not** physical disk savings. Removing
a sidecar may create reusable space while adding inline bytes elsewhere; it
need not shrink the DB file. This package has no space-reclamation operation.

The supplied historical source (2026-10-25) had 84,500 provider-event markers and
73,705 raw inline markers, but their eligible intersection is not established.
All marker kinds together occupied 123,871,232 bytes with indexes; that is not
this package's removable amount. Its 1,517,024,655 in-page unused bytes are not
OS-reclaimable bytes or duplicate audit bytes. Raw + activities dominate the
historical file. This small lossless cost control does not clear that backlog,
prove a current 48-hour target, or replace existing qualified content retention
and later application-coordinated space reclamation.

Deploying the new daemon and migrating a private copy are separate decisions.
The old runtime can still read raw headers/full audits; no schema or public API
change is required. Before any binary rollback, keep old maintenance disabled:
old audit preparation may add provider-event sidecars again. Never roll an old
whole DB over current work. Current host/native/input preservation, journal
measurement, reviewed controlled restart and planned-stop authorization remain
the operator's responsibility. This source package does not execute them.
