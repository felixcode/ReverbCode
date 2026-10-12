package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// PatchProviderProfiles changes only the supplied JSON branches under the
// same writer transaction used to validate their combined configuration.
func (s *Store) PatchProviderProfiles(ctx context.Context, id string, profiles *map[string]domain.ProviderProfile, active *string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw string
	if err = tx.QueryRowContext(ctx, "SELECT coalesce(config,'{}') FROM projects WHERE id=? AND archived_at IS NULL", id).Scan(&raw); err != nil {
		return err
	}
	var cfg domain.ProjectConfig
	if err = json.Unmarshal([]byte(raw), &cfg); err != nil {
		return err
	}
	if profiles != nil {
		cfg.ProviderProfiles = *profiles
	}
	if active != nil {
		cfg.ActiveProviderProfile = *active
	}
	if err = cfg.ValidateProviderProfiles(); err != nil {
		return err
	}
	if profiles != nil {
		data, err := json.Marshal(*profiles)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE projects SET config=json_set(coalesce(config,'{}'),'$.providerProfiles',json(?)) WHERE id=?", string(data), id); err != nil {
			return err
		}
	}
	if active != nil {
		if _, err = tx.ExecContext(ctx, "UPDATE projects SET config=json_set(coalesce(config,'{}'),'$.activeProviderProfile',?) WHERE id=?", *active, id); err != nil {
			return err
		}
	}
	if profiles == nil && active == nil {
		return fmt.Errorf("provider patch is empty")
	}
	return tx.Commit()
}
