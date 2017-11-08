package bytestream

import (
	"io"
	"strings"

	"github.com/Sirupsen/logrus"

	"golang.org/x/net/context"
	"golang.org/x/sync/syncmap"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Taken from https://github.com/google/google-api-go-client/blob/master/transport/bytestream/internal/server.go

// ReadHandler reads from the Bytestream.
// Note: error returns must return an instance of grpc.rpcError unless otherwise handled in grpc-go/rpc_util.go.
// http://google.golang.org/grpc provides Errorf(code, fmt, ...) to create instances of grpc.rpcError.
// Note: Cancelling the context will abort the stream ("drop the connection"). Consider returning a non-nil error instead.
type ReadHandler interface {
	// GetReader provides an io.ReaderAt, which will not be retained by the Server after the pb.ReadRequest.
	GetReader(ctx context.Context, name string) (io.ReaderAt, error)
	// Close does not have to do anything, but is here for if the io.ReaderAt wants to call Close().
	Close(ctx context.Context, name string) error
}

// WriteHandler handles writes from the Bytestream. For example:
// Note: error returns must return an instance of grpc.rpcError unless otherwise handled in grpc-go/rpc_util.go.
// grpc-go/rpc_util.go provides the helper func Errorf(code, fmt, ...) to create instances of grpc.rpcError.
// Note: Cancelling the context will abort the stream ("drop the connection"). Consider returning a non-nil error instead.
type WriteHandler interface {
	// GetWriter provides an io.Writer that is ready to write at initOffset.
	// The io.Writer will not be retained by the Server after the pb.WriteRequest.
	GetWriter(ctx context.Context, name string, initOffset int64) (io.Writer, error)
	// Close does not have to do anything, but is related to Server.AllowOverwrite. Or if the io.Writer simply wants a Close() call.
	// Close is called when the server receives a pb.WriteRequest with finish_write = true.
	// If Server.AllowOverwrite == true then Close() followed by GetWriter() for the same name indicates the name is being overwritten, even if the initOffset is different.
	Close(ctx context.Context, name string) error
}

type ByteStreamSrv struct {
	readHandler  ReadHandler
	writeHandler WriteHandler
	status       syncmap.Map

	digestMap syncmap.Map

	AllowOverwrite bool
}

func NewByteStreamSrv(r ReadHandler, w WriteHandler) *ByteStreamSrv {
	return &ByteStreamSrv{
		readHandler:    r,
		writeHandler:   w,
		status:         syncmap.Map{},
		AllowOverwrite: true,
	}
}

func (b *ByteStreamSrv) Read(in *bytestream.ReadRequest, stream bytestream.ByteStream_ReadServer) error {
	if b.readHandler == nil {
		return grpc.Errorf(codes.Unimplemented, "instance of NewServer(readHandler = nil) rejects all reads")
	}
	if in == nil {
		return grpc.Errorf(codes.Internal, "Read(ReadRequest == nil)")
	}
	if in.ResourceName == "" {
		return grpc.Errorf(codes.InvalidArgument, "ReadRequest: empty or missing resource_name")
	}

	reader, err := b.readHandler.GetReader(stream.Context(), in.ResourceName)
	if err != nil {
		return err
	}
	if err = b.readFrom(in, reader, stream); err != nil {
		b.readHandler.Close(stream.Context(), in.ResourceName)
		return err
	}
	if err = b.readHandler.Close(stream.Context(), in.ResourceName); err != nil {
		return err
	}
	return nil
}

func (b *ByteStreamSrv) readFrom(request *bytestream.ReadRequest, reader io.ReaderAt, stream bytestream.ByteStream_ReadServer) error {
	limit := int(request.ReadLimit)
	if limit < 0 {
		return grpc.Errorf(codes.InvalidArgument, "Read(): read_limit=%d is invalid", limit)
	}
	offset := request.ReadOffset
	if offset < 0 {
		return grpc.Errorf(codes.InvalidArgument, "Read(): offset=%d is invalid", offset)
	}

	var buf []byte
	if limit > 0 {
		buf = make([]byte, limit)
	} else {
		buf = make([]byte, 1024*1024) // 1M buffer is reasonable.
	}
	bytesSent := 0
	for limit == 0 || bytesSent < limit {
		n, err := reader.ReadAt(buf, offset)
		if n > 0 {
			if err := stream.Send(&bytestream.ReadResponse{Data: buf[:n]}); err != nil {
				return grpc.Errorf(grpc.Code(err), "Send(resourceName=%q offset=%d): %v", request.ResourceName, offset, grpc.ErrorDesc(err))
			}
		} else if err == nil {
			return grpc.Errorf(codes.Internal, "nil error on empty read: io.ReaderAt contract violated")
		}
		offset += int64(n)
		bytesSent += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return grpc.Errorf(codes.Unknown, "ReadAt(resourceName=%q offset=%d): %v", request.ResourceName, offset, err)
		}
	}
	return nil
}

func (b *ByteStreamSrv) Write(stream bytestream.ByteStream_WriteServer) error {
	for {
		writeReq, err := stream.Recv()
		if err == io.EOF {
			// io.EOF errors are a non-error for the Write() caller.
			return nil
		} else if err != nil {
			return grpc.Errorf(codes.Unknown, "stream.Recv() failed: %v", err)
		}
		if b.writeHandler == nil {
			return grpc.Errorf(codes.Unimplemented, "instance of NewServer(writeHandler = nil) rejects all writes")
		}

		// I'm not sure how we're supposed to infer the random uuid set by the client
		// so this removes it :shrug:
		// uploads/%s/blobs/%s/%d
		// -->
		// blobs/%s/%d
		r := strings.Split(writeReq.ResourceName, "/")
		writeReq.ResourceName = strings.Join(r[2:], "/")
		logrus.Infof("[BYTESTREAM] [WRITE] %s", writeReq.ResourceName)
		var status *bytestream.QueryWriteStatusResponse
		s, ok := b.status.Load(writeReq.ResourceName)
		if !ok {
			// writeReq.ResourceName is a new resource name.
			if writeReq.ResourceName == "" {
				return grpc.Errorf(codes.InvalidArgument, "WriteRequest: empty or missing resource_name:%s", writeReq)
			}
			status = &bytestream.QueryWriteStatusResponse{
				CommittedSize: writeReq.WriteOffset,
			}
			b.status.Store(writeReq.ResourceName, status)
		} else {
			status, ok := s.(*bytestream.QueryWriteStatusResponse)
			if !ok {
				return grpc.Errorf(codes.Internal, "type check failed")
			}
			// writeReq.ResourceName has already been seen by this server.
			if status.Complete {
				if !b.AllowOverwrite {
					return grpc.Errorf(codes.InvalidArgument, "%q finish_write = true already, got %d byte WriteRequest and Server.AllowOverwrite = false",
						writeReq.ResourceName, len(writeReq.Data))
				}
				// Truncate the resource stream.
				status.Complete = false
				status.CommittedSize = writeReq.WriteOffset
			}
		}
		logrus.Infoln("writeReq:", writeReq)
		logrus.Infoln("status:", status)

		if writeReq.WriteOffset != status.CommittedSize {
			return grpc.Errorf(codes.FailedPrecondition, "%q write_offset=%d differs from server internal committed_size=%d",
				writeReq.ResourceName, writeReq.WriteOffset, status.CommittedSize)
		}

		// // WriteRequest with empty data is ok.
		// if len(writeReq.Data) != 0 {
		writer, err := b.writeHandler.GetWriter(stream.Context(), writeReq.ResourceName, status.CommittedSize)
		if err != nil {
			return grpc.Errorf(codes.Internal, "GetWriter(%q): %v", writeReq.ResourceName, err)
		}
		wroteLen, err := writer.Write(writeReq.Data)
		if err != nil {
			return grpc.Errorf(codes.Internal, "Write(%q): %v", writeReq.ResourceName, err)
		}
		status.CommittedSize += int64(wroteLen)
		// }

		if writeReq.FinishWrite {
			r := &bytestream.WriteResponse{CommittedSize: status.CommittedSize}
			// Note: SendAndClose does NOT close the server stream.
			if err = stream.SendAndClose(r); err != nil {
				return grpc.Errorf(codes.Internal, "stream.SendAndClose(%q, WriteResponse{ %d }): %v", writeReq.ResourceName, status.CommittedSize, err)
			}
			status.Complete = true
			// if status.CommittedSize == 0 {
			// 	logrus.Warnf("write handler close 0 bytes written: %s", writeReq)
			// 	return nil
			// 	return grpc.Errorf(codes.FailedPrecondition, "writeHandler.Close(%q): 0 bytes written", writeReq.ResourceName)
			// }
			if err = b.writeHandler.Close(stream.Context(), writeReq.ResourceName); err != nil {
				return grpc.Errorf(codes.Internal, "writeHandler.Close(%q): %v", writeReq.ResourceName, err)
			}
			logrus.Infoln("Finished write for %s", writeReq.ResourceName)
		}
	}
}

func (b *ByteStreamSrv) QueryWriteStatus(ctx context.Context, in *bytestream.QueryWriteStatusRequest) (*bytestream.QueryWriteStatusResponse, error) {
	s, ok := b.status.Load(in.ResourceName)
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "resource_name not found: QueryWriteStatusRequest %v", in)
	}

	return s.(*bytestream.QueryWriteStatusResponse), nil
}
