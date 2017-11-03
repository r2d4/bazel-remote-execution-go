package cache

import "io"

type Cache interface {
	Get(path string, w io.Writer) (bool, error)
	Put(path string, r io.Reader) error

	// ?
	Contains(path string) (bool, error)
}