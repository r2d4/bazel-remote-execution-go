package main

import (
	"flag"
	"log"
	"net"

	"github.com/Sirupsen/logrus"

	"github.com/r2d4/bazel-remote-execution-go/server/action_cache"
	bs "github.com/r2d4/bazel-remote-execution-go/server/bytestream"
	"github.com/r2d4/bazel-remote-execution-go/server/cache/gcs_cache"
	"github.com/r2d4/bazel-remote-execution-go/server/cas"
	"github.com/r2d4/bazel-remote-execution-go/server/execution"
	"github.com/r2d4/bazel-remote-execution-go/server/watch"

	"google.golang.org/genproto/googleapis/bytestream"
	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
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

type srv struct {
	action_cache.ActionCacheSrv
	cas.CASSrv
	watch.WatchSrv
	execution.ExecutionSrv
	bs.ByteStreamSrv
}

func NewServer() (*srv, error) {
	cache, err := gcs_cache.NewGCSCache(bucket)
	if err != nil {
		return nil, err
	}

	acChan := make(chan watcher.Change)
	casChan := make(chan watcher.Change)

	return &srv{
		ActionCacheSrv: action_cache.ActionCacheSrv{
			Cache:      cache,
			ActionChan: acChan,
		},
		CASSrv: cas.CASSrv{
			Cache:   cache,
			CASChan: casChan,
		},
		ExecutionSrv: execution.ExecutionSrv{
			Cache: cache,
		},
		WatchSrv: watch.WatchSrv{
			ActionChan: acChan,
			CASChan:    casChan,
		},
		ByteStreamSrv: *bs.NewByteStreamSrv(cache, cache),
	}, nil
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
	s := grpc.NewServer(grpc.MaxMsgSize(10 << 16))
	impl, err := NewServer()
	if err != nil {
		log.Fatalf("error creating server: %s", err)
	}
	pb.RegisterExecutionServer(s, impl)
	pb.RegisterActionCacheServer(s, impl)
	pb.RegisterContentAddressableStorageServer(s, impl)
	watcher.RegisterWatcherServer(s, impl)
	bytestream.RegisterByteStreamServer(s, impl)

	// Register reflection service on gRPC server.
	reflection.Register(s)
	logrus.Infof("Listening on %s...", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
