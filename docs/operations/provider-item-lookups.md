# Provider-item point lookups

Streaming message lookup, append and settlement, and activity command-output / prose append and existence checks route nonempty provider item IDs to sqlc queries with an explicit `provider_item_id <> ''` predicate. This makes SQLite eligible to seek the existing unique `(conversation_id, provider_item_id)` partial indexes instead of reading a conversation sequence range for each delta.

Empty provider item IDs continue to use the original queries. In particular, multiple legacy empty-ID rows can still match an update, and missing rows retain their original result. This change does not reject empty IDs, allocate replacement IDs, batch or truncate deltas, change retention markers, or alter the queue / provider dispatch protocol. Existing activity settlement and activity lookup already use the indexed predicate. The context-reset boundary lookup also uses it because its sole caller supplies the nonempty generated context-reset identity.

No schema migration or new index is required. SQL source is `backend/internal/storage/sqlite/queries/conversations.sql`; regenerate with `npm run sqlc`. Focused modernc SQLite guards compare every row field against the original queries, cover empty / nonempty / missing identities, conversation isolation, retention stubs, cancelled and capped activities, and assert two-column partial-index seeks.

Operational CPU results remain workload-dependent. A passing query plan demonstrates the access path, not a production CPU guarantee; a normal daemon replacement and actual observation are separate operator actions.
