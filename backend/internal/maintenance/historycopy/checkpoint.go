//go:build linux

package historycopy

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// This is a conservative confirmed prefix, not authorization to reopen any DB.
// The current tool still requires a new private copy. A future continuation
// must separately bind a closed file hash/identity and its private custodian.
func saveCheckpoint(dir, sourceSHA, asOf, cutoff, stage, cursor string) error {
	body, err := json.Marshal(map[string]any{"protocol": 1, "source_sha256": sourceSHA, "as_of": asOf, "cutoff": cutoff, "stage": stage, "confirmed_cursor": cursor, "commit_state": "committed"})
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".history-checkpoint-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(body)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(name, filepath.Join(dir, "history-copy-checkpoint.json")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
