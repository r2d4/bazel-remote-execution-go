package cas

import (
	"bytes"
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/ptypes"
	"github.com/r2d4/bazel-remote-execution-go/server/cache"
	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	watcher "google.golang.org/genproto/googleapis/watcher/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type casSrv struct {
	c   cache.Cache
	cas chan watcher.Change
}

// FindMissingBlobs implements ContentAddressableStorage.FindMissingBlobs
func (s *casSrv) FindMissingBlobs(ctx context.Context, in *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	logrus.Infof("[FindMissingBlobs] %+v", in)
	missingBlobs := make(chan *pb.Digest)
	for _, req := range in.BlobDigests {
		var g errgroup.Group
		g.Go(func() error {
			found, err := s.c.Contains(fmt.Sprintf("bazel-cache/cas/%s", req.Hash))
			if err != nil {
				return err
			}
			if !found {
				missingBlobs <- req
				return nil
			}
			return nil
		})
	}
	var res []*pb.Digest
	for r := range missingBlobs {
		res = append(res, r)
	}
	return &pb.FindMissingBlobsResponse{
		MissingBlobDigests: res,
	}, nil
}

// BatchUpdateBlobs implements .BatchUpdateBlobs
func (s *casSrv) BatchUpdateBlobs(ctx context.Context, in *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	logrus.Infof("[BatchUpdateBlobs] %+v", in)
	var g errgroup.Group
	for _, req := range in.Requests {
		req := req
		g.Go(func() error {
			b := bytes.NewBuffer(req.Data)
			if err := s.c.Put(fmt.Sprintf("bazel-cache/cas/%s", req.ContentDigest.Hash), b); err != nil {
				return err
			}
			any, err := ptypes.MarshalAny(req)
			if err != nil {
				return err
			}
			s.cas <- watcher.Change{
				Element: req.ContentDigest.Hash,
				State:   watcher.Change_EXISTS,
				Data:    any,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, grpc.Errorf(codes.Internal, "writing cas: %v", err)
	}
	return &pb.BatchUpdateBlobsResponse{}, nil
}
