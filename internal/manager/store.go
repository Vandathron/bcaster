package manager

import "github.com/vandathron/bcaster/internal/cfg"

type Store struct {
}

func NewStore(config cfg.Store) (*Store, error) {
	return &Store{}, nil
}

func (s *Store) Publish()
