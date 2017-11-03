package gcs_cache

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/Sirupsen/logrus"
)

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

type GCS_Cache struct {
	bkt *storage.BucketHandle
	ctx context.Context
}

func (g *GCS_Cache) Get(path string, w io.Writer) (bool, error) {
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

func (g *GCS_Cache) Contains(path string) (bool, error) {
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

func (g *GCS_Cache) Put(path string, r io.Reader) error {
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
