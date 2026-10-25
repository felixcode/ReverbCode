# Session-list and retained-history reads

The history-retention sidecar has both a conversation index and a `(object_kind,
object_id)` primary key. Snapshot/page reads already know the exact returned
object IDs. Use those IDs as the outer VALUES relation, then look up the existing
primary key and retain the conversation/kind checks. This keeps marker reads in
the same snapshot and bounds their work to the page identities rather than the
conversation's raw-event audit history. Expiration, coordination and unknown-kind
semantics are unchanged.

Current-head review facts include terminal passes, so the partial running-review
index cannot cover their full contract. Rank the requested sessions' matching
current-head runs once by the original `(session, PR, target, harness)` key and
`created_at DESC, id DESC`. Joining the wanted-session relation after ranking
preserves duplicate input-session multiplicity. Empty/NULL heads remain excluded;
no status, body, history or result limit is added. The query still reads current
durable facts and does not cache display status. No schema/index migration or
ANALYZE is needed.

Focused modernc SQLite guards compare the full old/new result sets and inspect
EXPLAIN plans with the existing competing indexes. This change does not optimize
activity streamed-text appends or promise a final steady-state CPU percentage.
