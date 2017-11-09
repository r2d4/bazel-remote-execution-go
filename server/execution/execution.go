package execution

import (
	"bytes"
	"crypto/sha1"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"

	"golang.org/x/net/context"

	"github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/r2d4/bazel-remote-execution-go/server/cache"
	pb "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/genproto/googleapis/longrunning"
	watcher "google.golang.org/genproto/googleapis/watcher/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type ExecutionSrv struct {
	Cache cache.Cache

	ActionChan chan watcher.Change
	CASChan    chan watcher.Change
}

// Execute implements remote_execution.Execute
func (s *ExecutionSrv) Execute(ctx context.Context, in *pb.ExecuteRequest) (*longrunning.Operation, error) {
	logrus.Infof("[Execute] %+v", in)
	meta := &pb.ExecuteOperationMetadata{
		Stage:            pb.ExecuteOperationMetadata_QUEUED,
		ActionDigest:     in.Action.CommandDigest,
		StdoutStreamName: fmt.Sprintf("%s-stdout", in.Action.CommandDigest),
		StderrStreamName: fmt.Sprintf("%s-stderr", in.Action.CommandDigest),
	}
	m, err := ptypes.MarshalAny(meta)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "marshalling ExecuteOperationMetadata to protobuf.Any %s", err)
	}
	logrus.Info("Downloading input tree")
	if err := s.DownloadInputTree(ctx, in); err != nil {
		return nil, err
	}
	logrus.Info("Finished with input tree")

	cmd, err := s.GetCommand(ctx, in.Action)
	if err != nil {
		return nil, err
	}
	logrus.Info(cmd)

	go func() {
		ar, err := s.run(cmd, in)
		if err != nil {
			logrus.Warnf("Command %s failed: %s", cmd, err)
			return
		}
		meta := &pb.ExecuteOperationMetadata{
			Stage:        pb.ExecuteOperationMetadata_COMPLETED,
			ActionDigest: in.Action.CommandDigest,
		}
		m, err := ptypes.MarshalAny(meta)
		if err != nil {
			return
		}

		resp := &pb.ExecuteResponse{
			Result: ar,
		}

		respAny, err := ptypes.MarshalAny(resp)
		if err != nil {
			logrus.Warn("error: %s", err)
			return
		}

		op := &longrunning.Operation{
			Name:     in.Action.CommandDigest.Hash,
			Metadata: m,
			Done:     true,
			Result: &longrunning.Operation_Response{
				Response: respAny,
			},
		}

		opAny, err := ptypes.MarshalAny(op)
		if err != nil {
			return
		}
		s.CASChan <- watcher.Change{
			Element: in.Action.CommandDigest.Hash,
			State:   watcher.Change_EXISTS,
			Data:    opAny,
		}
	}()

	logrus.Info("returning long running op")
	return &longrunning.Operation{
		Name:     in.Action.CommandDigest.Hash,
		Metadata: m,
	}, nil
}

func (s *ExecutionSrv) run(c *pb.Command, in *pb.ExecuteRequest) (*pb.ActionResult, error) {
	outDirs := make([]*pb.OutputDirectory, len(in.Action.OutputDirectories))
	outFiles := make([]*pb.OutputFile, len(in.Action.OutputFiles))
	res := &pb.ActionResult{
		OutputDirectories: outDirs,
		OutputFiles:       outFiles,
	}
	cmd := exec.Command(c.Arguments[0], c.Arguments[1:]...)

	var stdout, stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		return nil, err
	}
	for i, path := range in.Action.OutputDirectories {
		digest, err := sha1digest(path)
		// even IsNotExists is bad here
		if err != nil {
			return nil, err
		}
		o := &pb.OutputDirectory{
			Path:   in.Action.OutputDirectories[i],
			Digest: digest,
		}

		outDirs[i] = o

		var b bytes.Buffer
		enc := gob.NewEncoder(&b)
		if err := enc.Encode(o); err != nil {
			return nil, grpc.Errorf(codes.Internal, "%v", err)
		}
		if err := s.Cache.Upload(digest, bytes.NewReader(b.Bytes())); err != nil {
			return nil, err
		}
	}
	for i, path := range in.Action.OutputFiles {
		digest, err := sha1digest(path)
		// even IsNotExists is bad here
		if err != nil {
			return nil, err
		}
		// todo If its small enough just put it in the content field.
		if digest.SizeBytes < 10<<4 {
			//
		}
		o := &pb.OutputFile{
			Path:   in.Action.OutputFiles[i],
			Digest: digest,
		}
		outFiles[i] = o

	}

	res.ExitCode = 0
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(res); err != nil {
		return nil, grpc.Errorf(codes.Internal, "%v", err)
	}

	h := sha1.New()
	_, err := h.Write(b.Bytes())
	if err != nil {
		return nil, err
	}
	actionDigest := &pb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: int64(len(b.Bytes())),
	}

	if err := s.Cache.Upload(actionDigest, bytes.NewReader(b.Bytes())); err != nil {
		return nil, err
	}

	logrus.Info("Returning action result for %s: %s", in, res)
	return res, nil
}

func sha1digest(path string) (*pb.Digest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &pb.Digest{
		SizeBytes: fi.Size(),
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
	}, nil
}

func (s *ExecutionSrv) GetCommand(ctx context.Context, in *pb.Action) (*pb.Command, error) {
	var b bytes.Buffer
	if err := s.Cache.Get(in.CommandDigest, &b); err != nil {
		return nil, grpc.Errorf(codes.Internal, "Error getting CAS dir: %+s", in)
	}

	var c pb.Command
	if err := proto.Unmarshal(b.Bytes(), &c); err != nil {
		return nil, err
	}

	return &c, nil
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
	logrus.Infof("File %s exists locally already", fpath)
	digest, err := sha1digest(fpath)
	// If the file exists, check if the hash and size match
	if err == nil {
		if node.Digest.Hash == digest.Hash && node.Digest.SizeBytes == digest.SizeBytes {
			logrus.Infof("File %s already exists locally, skipping", fpath)
			return nil
		}
	}
	// Some error other than is not exists
	if err != nil && !os.IsNotExist(err) {
		return err
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
