package storage

import (
	"context"
	"fmt"
	"io"
	"strconv"
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

		if !strings.HasSuffix(obj.Name(), ".md") {
			continue
		}

		filePaths = append(filePaths, strings.TrimPrefix(obj.Name(), sc.prefix))
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

	geolocationParts := strings.Split(metadata.Geolocation, " ")
	if (len(geolocationParts) == 1 && geolocationParts[0] != "") || len(geolocationParts) >= 4 {
		return fmt.Errorf("invalid geolocation format, expecting '{x} {y} [areaError]' or an empty string")
	}
	if len(geolocationParts) >= 2 {
		if _, err := strconv.ParseFloat(geolocationParts[0], 64); err != nil {
			return fmt.Errorf("invalid geolocation parameter, expected float for X: %w", err)
		}
		if _, err := strconv.ParseFloat(geolocationParts[1], 64); err != nil {
			return fmt.Errorf("invalid geolocation parameter, expected float for Y: %w", err)
		}
	}
	if len(geolocationParts) == 3 {
		if _, err := strconv.ParseFloat(geolocationParts[2], 64); err != nil {
			return fmt.Errorf("invalid geolocation parameter, expected float for area error: %w", err)
		}
	}

	attrs := &b2.Attrs{
		ContentType: "text/markdown; charset=utf-8",
		Info: map[string]string{
			"title":                     metadata.Title,
			"short-description":         metadata.ShortDescription,
			"action-date":               metadata.ActionDate,
			"published-time":            metadata.PublishedTime.Format(time.RFC3339),
			"thumbnail":                 metadata.Thumbnail,
			"tags":                      strings.Join(metadata.Tags, ","),
			"geolocation":               metadata.Geolocation,
			"metadata-last-update-sha1": oldAttrs.SHA1,
		}}

	reader := obj.NewReader(context.Background())
	content := make([]byte, oldAttrs.Size)
	if _, err = reader.Read(content); err != nil {
		return fmt.Errorf("failed to read an object back for writing (required for attribute setting): %w", err)
	}

	writer := obj.NewWriter(context.Background(), b2.WithAttrsOption(attrs))
	defer writer.Close()
	if _, err := writer.Write(content); err != nil {
		return fmt.Errorf("failed to write an object back after attribute settings: %w", err)
	}

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
