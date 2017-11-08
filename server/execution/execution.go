package execution

import (
	"bytes"
	"crypto/sha1"
	"io"
	"os"
	"path"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
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
	logrus.Info("Downloading input tree")
	if err := s.DownloadInputTree(ctx, in); err != nil {
		return nil, err
	}

	return &longrunning.Operation{
		Metadata: m,
	}, nil
}

func (s *ExecutionSrv) DownloadInputTree(ctx context.Context, in *pb.ExecuteRequest) error {
	//TODO sandbox?
	outDir, err := os.Getwd()
	if err != nil {
		return err
	}
	return s.downloadDirRecursive(ctx, in.Action.InputRootDigest, outDir)
}

func (s *ExecutionSrv) downloadDirRecursive(ctx context.Context, in *pb.Digest, dirpath string) error {
	var b bytes.Buffer
	if err := s.Cache.Get(in, &b); err != nil {
		return grpc.Errorf(codes.Internal, "Error getting CAS dir: %+s", in)
	}
	var dir pb.Directory

	if err := proto.Unmarshal(b.Bytes(), &dir); err != nil {
		return err
	}

	logrus.Info("downloadDirRecursive:", dir.String())
	for _, dir := range dir.Directories {
		if err := s.downloadDirRecursive(ctx, dir.Digest, path.Join(dirpath, dir.Name)); err != nil {
			return grpc.Errorf(codes.Internal, "error downloading directories: %s", err)
		}
	}

	for _, file := range dir.Files {
		if err := s.downloadFile(ctx, file, dirpath); err != nil {
			return grpc.Errorf(codes.Internal, "error downloading files: %s", err)
		}
	}

	return nil
}

func (s *ExecutionSrv) downloadFile(ctx context.Context, node *pb.FileNode, dirpath string) error {
	fpath := path.Join(dirpath, node.Name)
	if err := os.MkdirAll(dirpath, 0777); err != nil {
		return nil
	}
	if node.Digest.SizeBytes == 0 {
		logrus.Infof("File %s is empty, creating instead of fetching it", fpath)
		f, err := os.Create(fpath)
		if err != nil {
			return err
		}
		defer f.Close()
		return nil
	}
	fi, err := os.Stat(fpath)
	// File exists, check if it matches
	if err == nil {
		logrus.Infof("File %s exists locally already", fpath)
		f, err := os.Open(fpath)
		if err != nil {
			return err
		}
		defer f.Close()

		h := sha1.New()
		if _, err := io.Copy(h, f); err != nil {
			return err
		}
		if node.Digest.Hash == string(h.Sum(nil)) && node.Digest.SizeBytes == fi.Size() {
			logrus.Infof("File %s already exists locally, skipping", fpath)
			return nil
		}
	}
	logrus.Infof("Downloading file %s", fpath)

	f, err := os.Create(fpath)
	if err != nil {
		return err
	}

	if err := s.Cache.Get(node.Digest, f); err != nil {
		return grpc.Errorf(codes.Internal, "Error getting CAS file: %+s", node)
	}

	defer f.Close()

	if node.IsExecutable {
		if err := os.Chmod(fpath, 0777); err != nil {
			return err
		}
	}

	return nil
}
