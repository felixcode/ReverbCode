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
