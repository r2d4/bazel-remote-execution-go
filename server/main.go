package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"cloud.google.com/go/storage"

	"google.golang.org/grpc/codes"

	"github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/ptypes"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"
	watcher "google.golang.org/genproto/googleapis/watcher/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	port = ":50051"
)

var (
	bucket    string
	verbosity string
)

// execSvr is used to implement remote_execution.ExecutionServer
type srv struct {
	c cache

	// Changes to the action queue
	ac chan watcher.Change

	// Changes to the CAS queue
	cas chan watcher.Change
}

// Execute implements remote_execution.Execute
func (s *srv) Execute(ctx context.Context, in *pb.ExecuteRequest) (*longrunning.Operation, error) {
	logrus.Infof("[Execute] %+v", in)
	meta := &pb.ExecuteOperationMetadata{
		Stage:        pb.ExecuteOperationMetadata_QUEUED,
		ActionDigest: in.Action.CommandDigest,
	}
	m, err := ptypes.MarshalAny(meta)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "marshalling ExecuteOperationMetadata to protobuf.Any %s", err)
	}
	return &longrunning.Operation{
		Metadata: m,
	}, nil
}

// GetActionResult implements ActionCacheServer.GetActionResult
func (s *srv) GetActionResult(ctx context.Context, in *pb.GetActionResultRequest) (*pb.ActionResult, error) {
	logrus.Infof("[GetActionResult] %+v", in)
	var b bytes.Buffer
	err := s.c.get(fmt.Sprintf("bazel-cache/ac/%s", in.ActionDigest.Hash), &b)
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
func (s *srv) UpdateActionResult(ctx context.Context, in *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	logrus.Infof("[UpdateActionResult] %+v", in)
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(in.ActionResult); err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	if err := s.c.put(fmt.Sprintf("bazel-cache/ac/%s", in.ActionDigest.Hash), &b); err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}
	return nil, grpc.Errorf(codes.NotFound, "")
}

// FindMissingBlobs implements ContentAddressableStorage.FindMissingBlobs
func (s *srv) FindMissingBlobs(ctx context.Context, in *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	logrus.Infof("[FindMissingBlobs] %+v", in)
	missingBlobs := make(chan *pb.Digest)
	for _, req := range in.BlobDigests {
		var g errgroup.Group
		g.Go(func() error {
			found, err := s.c.contains(fmt.Sprintf("bazel-cache/cas/%s", req.Hash))
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

type cache interface {
	get(path string, w io.Writer) (bool, error)
	put(path string, r io.Reader) error

	// ?
	contains(path string) (bool, error)
}

func NewGCSCache(bucketName string) (*gcs_cache, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bkt := client.Bucket(bucketName)
	return &gcs_cache{
		bkt: bkt,
		ctx: context.Background(),
	}, nil
}

type gcs_cache struct {
	bkt *storage.BucketHandle
	ctx context.Context
}

func (g *gcs_cache) get(path string, w io.Writer) (bool, error) {
	obj := g.bkt.Object(path)
	r, err := obj.NewReader(g.ctx)
	if err != nil {
		logrus.Infof("Cache miss on %s.", path)
		return false, nil
	}
	logrus.Infof("Cache hit on %s.", path)
	n, err := io.Copy(w, r)
	if err != nil {
		return false, err
	}
	logrus.Infof("Bytes copied from cache: %d", n)
	return true, nil
}

func (g *gcs_cache) contains(path string) (bool, error) {
	obj := g.bkt.Object(path)
	r, err := obj.NewReader(g.ctx)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return false, err
		}
		return false, nil
	}
	defer r.Close()
	return true, nil
}

func (g *gcs_cache) put(path string, r io.Reader) error {
	logrus.Infof("Adding %s to cache.", path)
	obj := g.bkt.Object(path)

	w := obj.NewWriter(g.ctx)
	n, err := io.Copy(w, r)
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	logrus.Infof("Bytes written: %d", n)
	return err
}

// BatchUpdateBlobs implements .BatchUpdateBlobs
func (s *srv) BatchUpdateBlobs(ctx context.Context, in *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	logrus.Infof("[BatchUpdateBlobs] %+v", in)
	var g errgroup.Group
	for _, req := range in.Requests {
		req := req
		g.Go(func() error {
			b := bytes.NewBuffer(req.Data)
			if err := s.c.put(fmt.Sprintf("bazel-cache/cas/%s", req.ContentDigest.Hash), b); err != nil {
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

// GetTree is deprecated
func (s *srv) GetTree(ctx context.Context, in *pb.GetTreeRequest) (*pb.GetTreeResponse, error) {
	logrus.Infof("[GetTree] %+v", in)
	return nil, nil
}

// Watch implements water/v1/watch service
func (s *srv) Watch(stream *watcher.Request, w watcher.Watcher_WatchServer) error {
	for {
		// Send finish message!
	}
	return nil
}

func main() {
	flag.StringVar(&verbosity, "verbosity", "warn", "Logging verbosity.")
	flag.StringVar(&bucket, "bucket", "", "GCS bucket to use as a bazel cache.")

	flag.Parse()

	if bucket == "" {
		log.Fatalln("Please provide a value for the --bucket flag.")
	}
	lvl, err := logrus.ParseLevel(verbosity)
	if err != nil {
		log.Fatalln("Unable to parse verbosity flag.")
	}
	logrus.SetLevel(lvl)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	cache, err := NewGCSCache(bucket)
	if err != nil {
		log.Fatalf("error bucket: %s", err)
	}
	impl := &srv{
		c:   cache,
		ac:  make(chan watcher.Change),
		cas: make(chan watcher.Change),
	}

	pb.RegisterExecutionServer(s, impl)
	pb.RegisterActionCacheServer(s, impl)
	pb.RegisterContentAddressableStorageServer(s, impl)
	watcher.RegisterWatcherServer(s, impl)
	// Register reflection service on gRPC server.
	reflection.Register(s)
	logrus.Infof("Listening on %s...", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
