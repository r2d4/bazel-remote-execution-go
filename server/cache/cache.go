package cache

import (
	"io"

	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
)

type Cache interface {
	Get(*pb.Digest, io.Writer) error
	Put(*pb.Digest, io.Reader) error
	// ?
	Contains(*pb.Digest) (bool, error)
}
