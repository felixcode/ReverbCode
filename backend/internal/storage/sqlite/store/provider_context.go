package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func providerTable(kind string) (string, error) {
	switch kind {
	case "session":
		return "sessions", nil
	case "review":
		return "review", nil
	}
	return "", fmt.Errorf("unknown provider owner kind")
}

// EnsureProviderContext is first-writer-wins; controller updates never rewrite
// the original environment. Callers must persist it before provider launch.
func (s *Store) EnsureProviderContext(ctx context.Context, kind, id string, proposed *domain.ProviderContext) (*domain.ProviderContext, error) {
	table, err := providerTable(kind)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(proposed)
	if err != nil || proposed == nil {
		return nil, fmt.Errorf("provider context is required")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.writeDB.ExecContext(ctx, "UPDATE "+table+" SET provider_context=? WHERE id=? AND provider_context=''", string(data), id)
	if err != nil {
		return nil, err
	}
	return readProviderContext(ctx, s.writeDB, table, id)
}

type providerReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func readProviderContext(ctx context.Context, db providerReader, table, id string) (*domain.ProviderContext, error) {
	var data string
	if err := db.QueryRowContext(ctx, "SELECT provider_context FROM "+table+" WHERE id=?", id).Scan(&data); err != nil {
		return nil, err
	}
	if data == "" {
		return nil, nil
	}
	var p domain.ProviderContext
	if err := json.Unmarshal([]byte(data), &p); err != nil {
		return nil, fmt.Errorf("invalid persisted provider context")
	}
	if p.Env == nil {
		return nil, fmt.Errorf("persisted provider env missing")
	}
	return &p, nil
}

func (s *Store) hydrateProviderSession(ctx context.Context, rec domain.SessionRecord) (domain.SessionRecord, error) {
	p, err := readProviderContext(ctx, s.readDB, "sessions", string(rec.ID))
	if err != nil {
		return rec, err
	}
	rec.Metadata.ProviderContext = p
	if p != nil {
		rec.ProviderProfile = p.Profile
		rec.ProviderLabel = p.Label
	}
	return rec, nil
}
func (s *Store) hydrateProviderSessions(ctx context.Context, rows []domain.SessionRecord) ([]domain.SessionRecord, error) {
	for i := range rows {
		var err error
		rows[i], err = s.hydrateProviderSession(ctx, rows[i])
		if err != nil {
			return nil, err
		}
	}
	return rows, nil
}
func (s *Store) hydrateProviderReview(ctx context.Context, r domain.Review) (domain.Review, error) {
	p, err := readProviderContext(ctx, s.readDB, "review", r.ID)
	if err != nil {
		return r, err
	}
	r.ProviderContext = p
	if p != nil {
		r.ProviderProfile = p.Profile
		r.ProviderLabel = p.Label
	}
	return r, nil
}
