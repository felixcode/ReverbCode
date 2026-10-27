# Running-review qualification index (window preparation)

Migration 0192 adds one nonunique partial index on `review_run(session_id)` with
`WHERE status = 'running'`. It serves the existing fresh qualification query
`SELECT 1 FROM review_run WHERE session_id=? AND status='running' LIMIT 1`,
including reviews whose target SHA and batch ID are empty. Existing unique and
batch indexes have extra predicates and cannot cover that general query.
Their definitions and review lifecycle rules remain unchanged. No review row,
status, eligibility rule or transaction cache changes in this migration.

The partial index stores running rows only. Settling a review removes its entry;
terminal history does not accumulate in this new index. This does not bound the
review table or any other metadata. Tests compare the unindexed/indexed result,
status transitions, the unchanged uniqueness guard and actual modernc query plan.
Small synthetic index-size/write timings are ordinary diagnostics, not a promise
of a production speed multiplier or index-build duration.

This package is **window-only preparation**. Do not install it or start its new
runtime on the current production database: normal `ao daemon` startup applies
embedded migrations. Apply only in the separately scheduled, drained AO database
replacement window with verified recovery material. Index creation needs a writer
transaction, reads current review metadata and consumes space; actual build cost
must be budgeted for that stopped source. No hot DDL, daemon restart, maintenance
frequency/budget change, checkpoint, VACUUM or notification is part of this work.

The accepted old closed-candidate test binary first opens a version-191 private
copy, then launches a separately SHA-bound AO binary. Its nonempty path checks
files after shutdown without reopening the version-192 copy through the old Store.
This can test 191→192 migration with a new runtime while leaving the original
candidate readonly. A source already at 192 is outside that old pre-open contract;
do not bypass it. Component source/SHA bindings must distinguish the unchanged
old test harness from the new runtime. Real native/provider compatibility remains
unverified. Normal existing maintenance policy already accepts versions 191/192.

Rolling back only this migration drops only its own nonunique index. The old
runtime still embeds a lower migration version, so it must not be blindly started
against a newer database. Follow the planned recovery boundary, preserving any
new startup/business facts; private boot observations do not authorize production
rollback or an old full-database overwrite.
