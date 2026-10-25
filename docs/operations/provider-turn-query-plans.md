# Provider-turn point reads and updates

A nonempty provider turn ID is unique within its conversation through the
existing partial index `idx_conversation_turns_provider`. Its defining predicate
must also be present in the point lookup/update SQL for SQLite to use it.

Nonempty lookup, diff and plan operations use generated dedicated queries with
`provider_turn_id <> ''`. Empty legacy IDs retain the exact original SQL and
matching behavior. The existing query set/transaction is passed through unchanged.
Generation, owner, branch, authentication, expiration CASE expressions and event
archive identity checks retain their original behavior; this adds no migration,
cache, history truncation, prompt replay or timeout change.

Tests compare full rows/results across empty/nonempty IDs, missing/cross-conversation
IDs, terminal/running state and expired markers, assert modernc query plans use the
existing unique partial index, and report paired synthetic 12,000-turn lookup costs.
The elapsed costs are diagnostic rather than a quiet-host performance guarantee.

A cold-resume error naming this query identifies the operation present at the
request deadline; it does not establish how much of the total deadline elapsed
inside that operation. Native initialization, snapshot and replay phase timings
must not be inferred from the final error alone.
