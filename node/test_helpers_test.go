package node

import "sync"

type failSecondSaveStorage struct {
	mu    sync.Mutex
	state PersistentState
	saves int
	err   error
}

func (s *failSecondSaveStorage) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, nil
}

func (s *failSecondSaveStorage) Save(state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.saves++

	if s.saves == 2 {
		return s.err
	}

	s.state = state
	return nil
}
