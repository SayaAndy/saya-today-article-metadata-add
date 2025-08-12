package main

import (
	"flag"
	"log/slog"
	"os"
	"sync"

	"github.com/SayaAndy/saya-today-article-metadata-add/config"
	"github.com/SayaAndy/saya-today-article-metadata-add/internal/frontmatter"
	"github.com/SayaAndy/saya-today-article-metadata-add/internal/storage"
)

var configPath = flag.String("c", "config.json", "Path to the configuration file")

func main() {
	flag.Parse()

	cfg := &config.Config{}
	if err := config.LoadConfig(*configPath, cfg); err != nil {
		slog.Error("fail to load configuration", slog.String("error", err.Error()))
		os.Exit(1)
	}

	slog.SetLogLoggerLevel(cfg.LogLevel)
	slog.Info("starting metadata extractor...")

	storageClient, err := storage.NewStorageClientMap[cfg.Storage.Type](&cfg.Storage)
	if err != nil {
		slog.Error("fail to initialize input client", slog.String("error", err.Error()))
		os.Exit(1)
	}

	generalLogger := slog.With(
		slog.String("storage_type", cfg.Storage.Type),
	)
	generalLogger.Info("initialized storage client")

	files, err := storageClient.Scan()
	if err != nil {
		generalLogger.Error("fail to scan input files", slog.String("error", err.Error()))
		os.Exit(1)
	}
	generalLogger.Info("scanned files", slog.Int("file_count", len(files)))

	semaphore := make(chan struct{}, cfg.MaxConcurrentJobs)
	var wg sync.WaitGroup
	wg.Add(len(files))

	for i, file := range files {
		semaphore <- struct{}{}
		go func(index int, inputName string) {
			defer wg.Done()
			defer func() { <-semaphore }()
			if !storageClient.FileHasChanged(file) {
				generalLogger.Debug("skipped a file because it has not changed since last parse", slog.String("file", file))
				return
			}
			generalLogger.Debug("processing a file", slog.String("file", file))

			reader, sz, err := storageClient.GetReader(file)
			if err != nil {
				generalLogger.Warn("fail to get reader for a file", slog.String("file", file), slog.String("error", err.Error()))
				return
			}
			defer reader.Close()

			content := make([]byte, sz)
			ln, err := reader.Read(content)
			if err != nil {
				generalLogger.Warn("fail to read content from a file", slog.String("file", file), slog.String("error", err.Error()))
				return
			}
			generalLogger.Debug("read content from a file",
				slog.String("file", file),
				slog.Int64("expected_size", sz),
				slog.Int("output_size", ln))

			metadata, _, err := frontmatter.ParseFrontmatter(content)
			if err != nil {
				generalLogger.Warn("fail to parse frontmatter of a file", slog.String("file", file), slog.String("error", err.Error()))
				return
			}

			if metadata == nil {
				generalLogger.Info("skip a file due to it not having metadata", slog.String("file", file))
				return
			}

			if err = storageClient.WriteMetadata(file, metadata); err != nil {
				generalLogger.Warn("fail to write metadata to a file", slog.String("file", file), slog.String("error", err.Error()))
			}
		}(i, file)
	}

	wg.Wait()
}
