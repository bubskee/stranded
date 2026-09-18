package node

import "errors"

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
	return PersistentState{}, errors.New("storage load not implemented")
}

func (s *fileStorage) Save(state PersistentState) error {
	return errors.New("storage save not implemented")
}
