-- sqlc parser-only representation of idempotent local schema additions.
-- Runtime applies them through ensureAlbanewsProviderSchema, outside conflicting
-- upstream migration version IDs; legacy ledger rows remain unmodified.
ALTER TABLE sessions ADD COLUMN provider_context TEXT NOT NULL DEFAULT '';
ALTER TABLE review ADD COLUMN provider_context TEXT NOT NULL DEFAULT '';
CREATE INDEX idx_review_run_running_session ON review_run (session_id) WHERE status = 'running';
