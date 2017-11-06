package gcs_cache

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"golang.org/x/net/context"
	"golang.org/x/sync/syncmap"

	"cloud.google.com/go/storage"
	"github.com/Sirupsen/logrus"
)

func NewGCSCache(bucketName string) (*GCS_Cache, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bkt := client.Bucket(bucketName)
	return &GCS_Cache{
		bkt: bkt,
		ctx: context.Background(),
		rws: syncmap.Map{},
	}, nil
}

type GCS_Cache struct {
	bkt *storage.BucketHandle
	ctx context.Context

	// Don't know how to do streaming requests to GCS so
	// just write to memory and then upload at the end
	rws syncmap.Map
}

func (g *GCS_Cache) Get(path string, w io.Writer) error {
	logrus.Infof("[CACHE] [GET] %s", path)
	obj := g.bkt.Object(path)
	r, err := obj.NewReader(g.ctx)
	if err != nil {
		logrus.Infof("[CACHE] [MISS] %s", path)
		return err
	}
	logrus.Infof("[CACHE] [HIT] %s", path)
	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return nil
}

func (g *GCS_Cache) Contains(path string) (bool, error) {
	logrus.Infof("[CACHE] [CONTAINS] %s", path)
	obj := g.bkt.Object(path)
	r, err := obj.NewReader(g.ctx)
	if err == storage.ErrObjectNotExist {
		logrus.Infof("[CACHE] [MISS] %s", path)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer r.Close()
	logrus.Infof("[CACHE] [HIT] %s", path)
	return true, nil
}

func (g *GCS_Cache) Put(path string, r io.Reader) error {
	logrus.Infof("[CACHE] [PUT] %s", path)
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

func (g *GCS_Cache) GetWriter(ctx context.Context, path string, initOffset int64) (io.Writer, error) {
	v, ok := g.rws.Load(path)
	if !ok {
		v = &readWriteSeeker{}
		g.rws.Store(path, v)
	}
	writer, ok := v.(*readWriteSeeker)
	if !ok {
		return nil, fmt.Errorf("type assertion")
	}
	if _, err := writer.Seek(initOffset, io.SeekStart); err != nil {
		return nil, err
	}
	return writer, nil
}

func (g *GCS_Cache) Close(ctx context.Context, path string) error {
	defer g.rws.Delete(path)
	v, ok := g.rws.Load(path)
	if !ok {
		logrus.Warnf("Called Close() on %s but is already not an open writer", path)
		return nil
	}
	rws, ok := v.(*readWriteSeeker)
	if !ok {
		return fmt.Errorf("type assertion")
	}

	r := rws.NewReader()
	if err := g.Put(path, r); err != nil {
		return err
	}

	return nil
}

// https://stackoverflow.com/a/45837752
type readWriteSeeker struct {
	buf []byte
	pos int
}

func (g *GCS_Cache) GetReader(_ context.Context, path string) (io.ReaderAt, error) {
	v, ok := g.rws.Load(path)
	if !ok {
		return nil, fmt.Errorf("no reader")
	}
	rws, ok := v.(*readWriteSeeker)
	if !ok {
		return nil, fmt.Errorf("type assertion")
	}
	if !ok {
		return nil, fmt.Errorf("No reader")
	}
	return bytes.NewReader(rws.buf), nil
}

func (w *readWriteSeeker) NewReader() io.Reader {
	return bytes.NewReader(w.buf)
}

func (w *readWriteSeeker) Write(p []byte) (n int, err error) {
	minCap := w.pos + len(p)
	if minCap > cap(w.buf) { // Make sure buf has enough capacity:
		buf2 := make([]byte, len(w.buf), minCap+len(p)) // add some extra
		copy(buf2, w.buf)
		w.buf = buf2
	}
	if minCap > len(w.buf) {
		w.buf = w.buf[:minCap]
	}
	copy(w.buf[w.pos:], p)
	w.pos += len(p)
	return len(p), nil
}

func (w *readWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	newPos, offs := 0, int(offset)
	switch whence {
	case io.SeekStart:
		newPos = offs
	case io.SeekCurrent:
		newPos = w.pos + offs
	case io.SeekEnd:
		newPos = len(w.buf) + offs
	}
	if newPos < 0 {
		return 0, errors.New("negative result pos")
	}
	w.pos = newPos
	return int64(newPos), nil
}
