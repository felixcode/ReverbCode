package persistenthost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// Read only the current prompt journal of the exact authenticated attachment.
// Metadata comes from every complete frame in the bounded file snapshot, never
// a guessed first-N prefix. Bytes and SDK replay/ACK state remain untouched.
func readACPRecoveryEvidence(ctx context.Context, dataDir string, expected Descriptor, nativeID string, budget int64) ([]string, error) {
	checkHost := func() error {
		current, err := readDescriptor(dataDir, expected.SessionID)
		if err != nil {
			return err
		}
		if current.PID != expected.PID || !current.StartedAt.Equal(expected.StartedAt) || current.Token != expected.Token || current.Address != expected.Address {
			return errors.New("recovery host identity changed")
		}
		return nil
	}
	if err := checkHost(); err != nil {
		return nil, err
	}
	if nativeID == "" || budget <= 0 {
		return nil, errors.New("recovery identity or byte budget unavailable")
	}
	dir, err := hostDir(dataDir, expected.SessionID)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "acp-prompt.journal")
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("recovery journal is not an owner-only regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, info) || info.Size() > budget || info.Size() > maxACPJournalBytes {
		return nil, errors.New("recovery journal identity or budget invalid")
	}
	reader := bufio.NewReader(io.NewSectionReader(file, 0, info.Size()))
	ids := make([]string, 0)
	seen := make(map[string]bool)
	nativeObserved := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		line, err := reader.ReadBytes('\n')
		if errors.Is(err, io.EOF) && len(line) == 0 {
			break
		}
		if err != nil {
			return nil, errors.New("recovery journal snapshot has an incomplete frame")
		}
		var frame struct {
			Params struct {
				SessionID string                     `json:"sessionId"`
				EventID   string                     `json:"eventId"`
				Meta      map[string]json.RawMessage `json:"_meta"`
			} `json:"params"`
		}
		if json.Unmarshal(line, &frame) != nil {
			return nil, errors.New("recovery journal frame invalid")
		}
		if frame.Params.SessionID != "" {
			if frame.Params.SessionID != nativeID {
				return nil, errors.New("recovery journal native identity mismatch")
			}
			nativeObserved = true
		}
		id := frame.Params.EventID
		if raw := frame.Params.Meta[ACPEventIDMetaKey]; len(raw) > 0 {
			if json.Unmarshal(raw, &id) != nil {
				return nil, errors.New("recovery journal event identity invalid")
			}
		}
		if id != "" && !seen[id] {
			ids = append(ids, id)
			seen[id] = true
		}
	}
	if info.Size() > 0 && (!nativeObserved || len(ids) == 0) {
		return nil, errors.New("recovery journal has no native event evidence")
	}
	if err := checkHost(); err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if after.Size() < info.Size() {
		return nil, errors.New("recovery journal was reset")
	}
	return ids, nil
}
