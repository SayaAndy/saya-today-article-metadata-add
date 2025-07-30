package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Backblaze/blazer/b2"
	"github.com/SayaAndy/saya-today-article-metadata-add/config"
	"github.com/SayaAndy/saya-today-article-metadata-add/internal/frontmatter"
)

var _ StorageClient = &B2StorageClient{}

type B2StorageClient struct {
	prefix string
	bucket *b2.Bucket
	b2cl   *b2.Client
}

func NewB2StorageClient(cfg *config.StorageConfig) (StorageClient, error) {
	if cfg.Type != "b2" {
		return nil, fmt.Errorf("invalid storage type for B2InputClient")
	}
	b2cfg := cfg.Config.(*config.B2Config)

	b2cl, err := b2.NewClient(context.Background(), b2cfg.KeyID, b2cfg.ApplicationKey)
	if err != nil {
		return nil, err
	}

	bucket, err := b2cl.Bucket(context.Background(), b2cfg.BucketName)
	if err != nil {
		return nil, err
	}

	return &B2StorageClient{b2cl: b2cl, bucket: bucket, prefix: b2cfg.Prefix}, nil
}

func (sc *B2StorageClient) Scan() ([]string, error) {
	filePaths := []string{}

	iter := sc.bucket.List(context.Background(), b2.ListPrefix(sc.prefix))

	for iter.Next() {
		obj := iter.Object()
		if obj == nil {
			return nil, fmt.Errorf("failed to reference object in B2 bucket")
		}

		attrs, err := obj.Attrs(context.Background())
		if err != nil {
			return nil, fmt.Errorf("get attributes for object: %w", err)
		}

		if attrs.Status != b2.Uploaded {
			continue
		}

		name := obj.Name()

		nameParts := strings.Split(name, ".")
		if len(nameParts) < 2 {
			continue
		}

		ext := strings.ToLower(nameParts[len(nameParts)-1])
		if ext != "md" {
			continue
		}

		filePaths = append(filePaths, strings.TrimPrefix(name, sc.prefix))
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterate over B2 objects: %w", err)
	}

	return filePaths, nil
}

func (sc *B2StorageClient) GetReader(path string) (io.ReadCloser, int64, error) {
	obj := sc.bucket.Object(sc.prefix + path)
	if obj == nil {
		return nil, 0, fmt.Errorf("failed to reference object in B2 bucket")
	}
	attrs, err := obj.Attrs(context.Background())
	if err != nil {
		return nil, 0, fmt.Errorf("error getting attributes of an object: %w", err)
	}

	return obj.NewReader(context.Background()), attrs.Size, nil
}

func (sc *B2StorageClient) WriteMetadata(path string, metadata *frontmatter.Metadata) error {
	obj := sc.bucket.Object(sc.prefix + path)
	if obj == nil {
		return fmt.Errorf("failed to reference object in B2 bucket")
	}
	oldAttrs, err := obj.Attrs(context.Background())
	if err != nil {
		return fmt.Errorf("error getting attributes of an object: %w", err)
	}

	attrs := &b2.Attrs{Info: map[string]string{
		"title":                     metadata.Title,
		"short-description":         metadata.ShortDescription,
		"action-date":               metadata.ActionDate,
		"published-time":            metadata.PublishedTime.Format(time.RFC3339),
		"thumbnail":                 metadata.Thumbnail,
		"tags":                      strings.Join(metadata.Tags, ","),
		"metadata-last-update-sha1": oldAttrs.SHA1,
	}}

	writer := obj.NewWriter(context.Background(), b2.WithAttrsOption(attrs))
	writer.Close()

	return nil
}

func (sc *B2StorageClient) FileHasChanged(path string) bool {
	obj := sc.bucket.Object(sc.prefix + path)
	if obj == nil {
		return true
	}

	attrs, err := obj.Attrs(context.Background())
	if err != nil {
		return true
	}

	lastUpdateSha1, ok := attrs.Info["metadata-last-update-sha1"]
	if !ok {
		return true
	}

	return attrs.SHA1 == lastUpdateSha1
}
