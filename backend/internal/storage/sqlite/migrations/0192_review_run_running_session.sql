-- General running-review qualification includes empty target_sha and batch_id.
-- Keep the existing running uniqueness guard and all historical review rows.

-- +goose Up
CREATE INDEX idx_review_run_running_session
    ON review_run (session_id)
    WHERE status = 'running';

-- +goose Down
DROP INDEX idx_review_run_running_session;
