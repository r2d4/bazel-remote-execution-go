package action_cache

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"golang.org/x/net/context"

	"cloud.google.com/go/storage"
	"github.com/Sirupsen/logrus"
	"github.com/r2d4/bazel-remote-execution-go/server/cache"
	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	watcher "google.golang.org/genproto/googleapis/watcher/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type ActionCacheSrv struct {
	Cache      cache.Cache
	ActionChan chan watcher.Change
}

// GetActionResult implements ActionCacheServer.GetActionResult
func (s *ActionCacheSrv) GetActionResult(ctx context.Context, in *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	logrus.Infof("[GetActionResult] %+v", in)
	var b bytes.Buffer
	err := s.Cache.Get(fmt.Sprintf("bazel-cache/ac/%s", in.ActionDigest.Hash), &b)
	if err == storage.ErrObjectNotExist {
		return nil, grpc.Errorf(codes.NotFound, "")
	}
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	var res *pb.ActionResult
	dec := gob.NewDecoder(&b)
	if err := dec.Decode(res); err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	return res, nil
}

// UpdateActionResult implements ActionCacheServer.UpdateActionResult
func (s *ActionCacheSrv) UpdateActionResult(ctx context.Context, in *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	logrus.Infof("[UpdateActionResult] %+v", in)
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(in.ActionResult); err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	if err := s.Cache.Put(fmt.Sprintf("bazel-cache/ac/%s", in.ActionDigest.Hash), &b); err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	return nil, grpc.Errorf(codes.NotFound, "")
}
