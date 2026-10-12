-- +goose Up
ALTER TABLE sessions ADD COLUMN provider_context TEXT NOT NULL DEFAULT '';
ALTER TABLE review ADD COLUMN provider_context TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE review DROP COLUMN provider_context;
ALTER TABLE sessions DROP COLUMN provider_context;
