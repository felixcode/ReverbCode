# Exact preserved-queue recovery

This local maintenance operation repairs an explicitly proven transport-cleanup
batch. It is not Retry, Send, automatic failed-turn recovery, or new intake.
It retains the original turn/message IDs, requested-at order, body/delivery bytes,
client identities, origins and sequence. Only state, error/completed lifecycle
facts and the owning controller generation change. It creates no migration.

Use `ao session recover-preserved-queue <session-id> --request /private/request.json`
with a regular 0600 metadata file. The CLI posts to
`POST /api/v1/dev/sessions/{sessionId}/preserved-queue/recover`. `/dev` is blocked
by the physical LAN/native-listener wrapper; the handler also rejects LAN-marked
contexts, Origin, Forwarded/X-Forwarded headers and non-loopback Host/peer. No
port is hardcoded. An nginx loopback peer is not authorization to this operation.

The v1 request contains `protocol`, `conversationId`, `branchId`, `nativeId`,
`boundTurnId`, `boundProviderTurnId`, `failureAt`, `beforeSnapshotSha256`, and
`turns`. Every turn contains `id`, `previousGeneration`, original `requestedAt`,
`beforeState: "queued"`, and `inputs`. Each input contains `id`, `clientMessageId`,
`clientPayloadHash`, `textSha256`, `deliverySha256`. Hashes are lowercase SHA256;
legacy client ID/hash may both be empty but must match the original stored facts.
Text/delivery hashes are always required. Go timestamp strings are copied exactly
from the incident evidence, not normalized by a client. This request is generated
from verified evidence explicitly; diagnostic candidate files are not API bodies.
No prompt text is included. The JSON limit is 1MiB and the batch limit 4096 turns.

The service takes the owner start/stop gate, controller sendMu, then the existing
store writer transaction. It accepts only the ACP Claude deferred Bind-before-
StartDeferredTurn path, attached to the same authenticated live host, native ID,
active branch and current controller generation. The exact original bound prompt
must be running with the same pending/acked provider ID, or actually completed
with same-host journal IDs joined to an archived successful terminal. Unknown,
closed, disconnected, stale or competing owners are refused. No process-alive
heuristic or caller-supplied terminal claim substitutes for those proofs.

Every original candidate must still have the exact cleanup failure reason/time,
old generation/order, empty provider ID, NULL start, no rollback/promotion/retry
or successor, and the exact original user-input identities and content digests.
The whole batch is checked before any lifecycle update. The typed audit and
original updates commit atomically. A canonical request digest identifies the
audit; retry after response loss returns restored=0 even if normal dispatch has
since consumed rows. It never puts those rows back or drains on a retry.

Busy recovery sends nothing. Only proven completed+idle recovery invokes the
existing FIFO drain after commit, under sendMu and A's disconnected dispatch
fence; it does not construct a new prompt or turn. Dispatch failure leaves the
committed recovery auditable and is reported as `normal_drain_failed`. Explicit
Exit/Interrupt remains authoritative; no recovery is automatic. Production use
requires the operator's independently verified incident proof and root-managed
deployment; unit fixtures are not proof that a production batch was restored.
