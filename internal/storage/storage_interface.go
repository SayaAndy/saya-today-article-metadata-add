package storage

import (
	"io"

	"github.com/SayaAndy/saya-today-article-metadata-add/config"
	"github.com/SayaAndy/saya-today-article-metadata-add/internal/frontmatter"
)

type StorageClient interface {
	Scan() (paths []string, err error)
	GetReader(path string) (reader io.ReadCloser, sz int64, err error)
	WriteMetadata(path string, metadata *frontmatter.Metadata) error
	FileHasChanged(path string) bool
}

var NewStorageClientMap = map[string]func(cfg *config.StorageConfig) (StorageClient, error){
	"b2": NewB2StorageClient,
}
