# Expired AO history compatibility

This patch applies to the installed `800c493b` source. It does not import later
main changes, change persistent-host adoption, or run retention itself.

The maintenance application may clear terminal history bodies after its configured
retention period while preserving rows, owner/turn/item/client identities,
sequence/revision/branch, outcome, and billing/authentication facts. Its transaction
must first compare-and-set the source body, then insert the matching
`ao_history_retention` marker, and commit both together. Each marker contains only
irreversible digests, byte counts, field names and expiry facts. The literal object
kinds are extensible; message/activity/turn/session compatibility never treats an
`activity_output` or `provider_event` audit row as a full projection marker.

The writable store initializes this small sidecar and validates its exact shape.
It does not replay or replace the historical migration ledger. `retention_schema.sql`
is an additional sqlc parser schema; generated queries and API definitions must be
regenerated using the repository tools.

Snapshots read the source bodies and markers in the same read transaction. Expired
items expose `contentExpired`; their readable stub is `[AO history expired]`.
Session metadata exposes `promptExpired`. The exact original message/activity
fingerprint bytes are represented by SHA-256 and counted per logical turn during
native replay. Normal, unexpired fingerprint matching stays unchanged. Prompt
SHA-256 is used only in the existing ordered, unused-turn fallback after stronger
native/item/client identities. Equal text in a new turn remains a new message.
Coordination-message boundary classification is retained separately as a
`message_coordination` marker, without retaining its original body.

Body assignments use marker-aware SQL CASE expressions, so late deltas, settled
payloads, output, diff/plan and saved-prompt writes preserve the committed stub.
An expired logical terminal turn also suppresses body projection and strips the
known top-level prose fields before archiving a late provider event. Method,
provider identities, outcome, unknown facts and error nullness are preserved.
Native replay duplicate suppression runs before archive projection, including
changed item IDs. No global content-hash filter is used.

A retry of an expired prompt fails explicitly and asks for new content. An expired
saved prompt cannot be used for a fresh launch; restoring an existing native
conversation remains a separate operation. The UI edit availability is false for
expired messages.

## Capability and deployment

`GET /healthz` retains its existing PID, appRunId and startedAt. The running patched
daemon explicitly wires `historyRetention` containing `protocol: 1`, `schemaReady`,
`dbDevice`, `dbInode`, and Linux `processStartTicks`. A maintenance caller must check
all identity facts against its live daemon and target DB before enabling projection
expiry. Missing/false capability authorizes no projection clearing. Table presence
alone is not a capability proof.

Before first expiry, the old binary can be restored. After expiry, rollback must
use a binary with the same digest-aware compatibility; the original binary cannot
interpret stubs safely. Do not replace a live DB or WAL with an old whole-database
backup: doing so would lose subsequent inputs and queue state.

The deployment owner must capture actual running/queued input identities and
hashes, persistent host/native identities and health before and after a controlled
ordinary daemon StopAll/Close and live adoption. User Exit/Terminate is a different
operation. This patch does not perform deployment or database maintenance.

The two latest native checkpoint text fields remain a documented small exception
(each already capped at 16 KiB). Their existing normalization and truncated
prefix/suffix comparison cannot be replaced by a single full-text SHA without
changing the recovery protocol. Maintenance reports must disclose actual retained
checkpoint bytes; no claim of zero retained old prose is made.

Snapshot helpers share one transaction-bound query set, including branch and
visibility queries. Lazy authentication reconciliation completes before the read
snapshot is pinned. Retry/edit read their marker and source body in one read
transaction. Marker lookups use only loaded page IDs and projection kinds, so
unrelated raw-event audit markers do not inflate a small page.

Old read-only import/restore sources without the optional sidecar remain
unmodified. Their generated session readers substitute the fixed expiry expression
with false; snapshot readers skip absent markers. This adapts each query, including
transactional readers and fresh pool connections, without creating TEMP tables,
changing the historical schema, or writing a read-only source.

## Cold replay cost and legacy send responses

Cold import still reads/validates/reconciles native history before publication.
It checks bounded batches of exact nonempty `(conversation_id, provider_event_id)`
archive keys after reconciliation; a committed key means the existing atomic
archive+projection already won, so no body marshal, retention parse or writer is
needed for that event. Unseen IDs, new suffixes/items and empty IDs retain the
original path. The positive set lives only for that cold import batch, under the
current generation; an existing projection transaction supplies its own snapshot.
No transcript truncation, new reader protocol or increased timeout is involved.

The expiry query explicitly includes the nonempty predicates on all four native
ID columns, allowing SQLite to use their existing partial indexes. This changes
no expiry/terminal/owner rule or database schema. Legacy `/sessions/{id}/send`
now maps the same two typed not-ready failures as the conversation route to409;
other errors keep their prior classification. A200 acknowledges acceptance
(including durable queuing), not provider execution/delivery, and never authorizes
automatic resend. Provisioned workspace/controller facts must be distinguished
from the display readiness of a partially created session.
