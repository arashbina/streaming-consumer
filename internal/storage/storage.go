package storage

import "io"

type Storage interface {
	Store(r io.ReadCloser, path string) error
}
