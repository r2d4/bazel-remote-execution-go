package cas

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"golang.org/x/sync/errgroup"

	"github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/ptypes"
	"github.com/r2d4/bazel-remote-execution-go/server/cache"
	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	watcher "google.golang.org/genproto/googleapis/watcher/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type CASSrv struct {
	Cache   cache.Cache
	CASChan chan watcher.Change
}

// FindMissingBlobs implements ContentAddressableStorage.FindMissingBlobs
func (s *CASSrv) FindMissingBlobs(ctx context.Context, in *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	logrus.Infof("[FindMissingBlobs] %+v", in)
	missingBlobs := make(chan *pb.Digest, len(in.BlobDigests))
	var g errgroup.Group
	for _, req := range in.BlobDigests {
		req := req
		g.Go(func() error {
			digest := fmt.Sprintf("bazel-cache/cas/%s", req.Hash)
			contains, err := s.Cache.Contains(digest)
			if err != nil {
				logrus.Info("error %s", err)
				return err
			}
			if !contains {
				logrus.Infof("missing %v", req)
				missingBlobs <- req
				logrus.Infof("sent to %v to missingBlobs, new size %d", len(missingBlobs))
				return nil
			}
			return nil
		})
	}
	logrus.Info("Waiting for missing blobs")
	if err := g.Wait(); err != nil {
		return nil, grpc.Errorf(codes.Internal, "error finding missing blobs: %s", err)
	}
	close(missingBlobs)
	var res []*pb.Digest
	for r := range missingBlobs {
		res = append(res, r)
	}

	logrus.Infof("[FindMissingBlobs Response] %+v", res)
	return &pb.FindMissingBlobsResponse{
		MissingBlobDigests: res,
	}, nil
}

// BatchUpdateBlobs implements .BatchUpdateBlobs
func (s *CASSrv) BatchUpdateBlobs(ctx context.Context, in *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	logrus.Infof("[BatchUpdateBlobs] %+v", in)
	var g errgroup.Group
	for _, req := range in.Requests {
		req := req
		g.Go(func() error {
			b := bytes.NewBuffer(req.Data)
			if err := s.Cache.Put(fmt.Sprintf("bazel-cache/cas/%s", req.ContentDigest.Hash), b); err != nil {
				return err
			}
			any, err := ptypes.MarshalAny(req)
			if err != nil {
				return err
			}
			s.CASChan <- watcher.Change{
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

// GetTree is deprecated
func (s *CASSrv) GetTree(ctx context.Context, in *pb.GetTreeRequest) (*pb.GetTreeResponse, error) {
	logrus.Infof("[GetTree] %+v", in)
	return nil, nil
}
