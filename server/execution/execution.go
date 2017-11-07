package execution

import (
	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/ptypes"
	"github.com/r2d4/bazel-remote-execution-go/server/cache"
	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type ExecutionSrv struct {
	Cache cache.Cache
}

// Execute implements remote_execution.Execute
func (s *ExecutionSrv) Execute(ctx context.Context, in *pb.ExecuteRequest) (*longrunning.Operation, error) {
	logrus.Infof("[Execute] %+v", in)
	meta := &pb.ExecuteOperationMetadata{
		Stage:        pb.ExecuteOperationMetadata_QUEUED,
		ActionDigest: in.Action.CommandDigest,
	}
	m, err := ptypes.MarshalAny(meta)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "marshalling ExecuteOperationMetadata to protobuf.Any %s", err)
	}

	//TODO(r2d4): implement exection

	return &longrunning.Operation{
		Metadata: m,
	}, nil
}
