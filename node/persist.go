package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const persistentStateFile = "raft-state.json"

type Storage interface {
	Load() (PersistentState, error)
	Save(PersistentState) error
}

type fileStorage struct {
	dir string
}

func newFileStorage(dir string) *fileStorage {
	return &fileStorage{dir: dir}
}

func (s *fileStorage) Load() (PersistentState, error) {
	path := filepath.Join(s.dir, persistentStateFile)

	data, err := os.ReadFile(path)
	if err != nil {
		return PersistentState{}, fmt.Errorf("read persistent state: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("decode persistent state: %w", err)
	}

	return state, nil
}

func (s *fileStorage) Save(state PersistentState) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("create storage directory: %w", err)
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode persistent state: %w", err)
	}

	tmp, err := os.CreateTemp(s.dir, ".raft-state-*")
	if err != nil {
		return fmt.Errorf("create temporary state file: %w", err)
	}

	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write persistent state: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync persistent state: %w", err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close persistent state: %w", err)
	}

	path := filepath.Join(s.dir, persistentStateFile)
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace persistent state: %w", err)
	}

	dir, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open storage directory: %w", err)
	}
	defer dir.Close()

	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync storage directory: %w", err)
	}

	return nil
}
