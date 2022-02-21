package s3

import "io"

type Store struct {
}

func NewStore() (*Store, error) {
	return nil, nil
}

func (s *Store) Store(r io.ReadCloser, path string) error {
	//TODO implement me
	panic("implement me")
}
