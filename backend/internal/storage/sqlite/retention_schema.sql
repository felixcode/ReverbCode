-- sqlc parser schema; runtime initializes this small compatibility table without replaying migrations.
CREATE TABLE ao_history_retention (
 object_kind TEXT NOT NULL, object_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
 semantic_sha256 TEXT NOT NULL, prompt_sha256 TEXT NOT NULL,
 original_bytes INTEGER NOT NULL, original_sha256 TEXT NOT NULL,
 expired_at TEXT NOT NULL, cutoff_at TEXT NOT NULL, fields_json TEXT NOT NULL,
 PRIMARY KEY(object_kind, object_id));
