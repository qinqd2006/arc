package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// LocalBackend implements the Backend interface for local filesystem storage
type LocalBackend struct {
	basePath string
	config   LocalConfig
	metrics  Metrics
	logger   *logrus.Logger
}

// LocalConfig holds local storage configuration
type LocalConfig struct {
	BasePath string `toml:"base_path"`
	Database string `toml:"database"`
}

// NewLocalBackend creates a new local storage backend
func NewLocalBackend(config LocalConfig, logger *logrus.Logger) (*LocalBackend, error) {
	if config.BasePath == "" {
		config.BasePath = "./data/arc"
	}
	if config.Database == "" {
		config.Database = "default"
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &LocalBackend{
		basePath: config.BasePath,
		config:   config,
		logger:   logger,
	}, nil
}

// Put stores data at the given key
func (l *LocalBackend) Put(ctx context.Context, key string, data io.Reader, size int64) error {
	start := time.Now()
	defer func() {
		l.metrics.PutOperations++
		l.metrics.BytesWritten += size
		l.metrics.LastOperation = time.Now()
	}()

	fullPath := filepath.Join(l.basePath, key)

	// Create directory structure
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		l.metrics.ErrorCount++
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Create temporary file
	tempPath := fullPath + ".tmp"
	file, err := os.Create(tempPath)
	if err != nil {
		l.metrics.ErrorCount++
		return fmt.Errorf("failed to create temp file %s: %w", tempPath, err)
	}
	defer file.Close()

	// Copy data
	written, err := io.Copy(file, data)
	if err != nil {
		os.Remove(tempPath)
		l.metrics.ErrorCount++
		return fmt.Errorf("failed to write data to temp file %s: %w", tempPath, err)
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		os.Remove(tempPath)
		l.metrics.ErrorCount++
		return fmt.Errorf("failed to sync temp file %s: %w", tempPath, err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, fullPath); err != nil {
		os.Remove(tempPath)
		l.metrics.ErrorCount++
		return fmt.Errorf("failed to rename temp file %s to %s: %w", tempPath, fullPath, err)
	}

	l.logger.WithFields(logrus.Fields{
		"key":  key,
		"size": written,
		"path": fullPath,
		"took": time.Since(start),
	}).Debug("LocalBackend: Put completed")

	return nil
}

// Get retrieves data from the given key
func (l *LocalBackend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	defer func() {
		l.metrics.GetOperations++
		l.metrics.LastOperation = time.Now()
	}()

	fullPath := filepath.Join(l.basePath, key)

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			l.metrics.ErrorCount++
			return nil, ErrObjectNotFound
		}
		l.metrics.ErrorCount++
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}

	// Get file size for metrics
	stat, err := file.Stat()
	if err == nil {
		l.metrics.BytesRead += stat.Size()
	}

	l.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": fullPath,
		"took": time.Since(start),
	}).Debug("LocalBackend: Get completed")

	return file, nil
}

// Delete removes data at the given key
func (l *LocalBackend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer func() {
		l.metrics.DeleteOperations++
		l.metrics.LastOperation = time.Now()
	}()

	fullPath := filepath.Join(l.basePath, key)

	err := os.Remove(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrObjectNotFound
		}
		l.metrics.ErrorCount++
		return fmt.Errorf("failed to delete file %s: %w", fullPath, err)
	}

	l.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": fullPath,
		"took": time.Since(start),
	}).Debug("LocalBackend: Delete completed")

	return nil
}

// Exists checks if a key exists
func (l *LocalBackend) Exists(ctx context.Context, key string) (bool, error) {
	fullPath := filepath.Join(l.basePath, key)

	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		l.metrics.ErrorCount++
		return false, fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}

	return true, nil
}

// List returns objects with the given prefix
func (l *LocalBackend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	start := time.Now()
	defer func() {
		l.metrics.ListOperations++
		l.metrics.LastOperation = time.Now()
	}()

	fullPrefix := filepath.Join(l.basePath, prefix)

	var objects []ObjectInfo

	err := filepath.Walk(fullPrefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Get relative path from base path
		relPath, err := filepath.Rel(l.basePath, path)
		if err != nil {
			return err
		}

		// Convert to forward slashes for consistency
		key := filepath.ToSlash(relPath)

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         fmt.Sprintf("%d-%d", info.Size(), info.ModTime().Unix()),
		})

		return nil
	})

	if err != nil {
		if os.IsNotExist(err) {
			return []ObjectInfo{}, nil
		}
		l.metrics.ErrorCount++
		return nil, fmt.Errorf("failed to list directory %s: %w", fullPrefix, err)
	}

	// Sort objects by key for consistent ordering
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	l.logger.WithFields(logrus.Fields{
		"prefix": prefix,
		"count":  len(objects),
		"took":   time.Since(start),
	}).Debug("LocalBackend: List completed")

	return objects, nil
}

// ListWithRecursive returns objects with the given prefix, optionally recursive
func (l *LocalBackend) ListWithRecursive(ctx context.Context, prefix string, recursive bool) ([]ObjectInfo, error) {
	if recursive {
		return l.List(ctx, prefix)
	}

	start := time.Now()
	defer func() {
		l.metrics.ListOperations++
		l.metrics.LastOperation = time.Now()
	}()

	fullPrefix := filepath.Join(l.basePath, prefix)

	var objects []ObjectInfo

	// Read directory entries only (non-recursive)
	entries, err := os.ReadDir(fullPrefix)
	if err != nil {
		if os.IsNotExist(err) {
			return []ObjectInfo{}, nil
		}
		l.metrics.ErrorCount++
		return nil, fmt.Errorf("failed to read directory %s: %w", fullPrefix, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			l.logger.WithError(err).WithField("entry", entry.Name()).Warn("Failed to get file info")
			continue
		}

		// Get relative path from base path
		fullPath := filepath.Join(fullPrefix, entry.Name())
		relPath, err := filepath.Rel(l.basePath, fullPath)
		if err != nil {
			l.logger.WithError(err).WithField("path", fullPath).Warn("Failed to get relative path")
			continue
		}

		// Convert to forward slashes for consistency
		key := filepath.ToSlash(relPath)

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			ETag:         fmt.Sprintf("%d-%d", info.Size(), info.ModTime().Unix()),
		})
	}

	// Sort objects by key for consistent ordering
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	l.logger.WithFields(logrus.Fields{
		"prefix":    prefix,
		"count":     len(objects),
		"recursive": recursive,
		"took":      time.Since(start),
	}).Debug("LocalBackend: ListWithRecursive completed")

	return objects, nil
}

// PutBatch stores multiple files atomically
func (l *LocalBackend) PutBatch(ctx context.Context, files []BatchFile) error {
	start := time.Now()
	defer func() {
		l.metrics.LastOperation = time.Now()
	}()

	// For local storage, we use a two-phase commit approach
	// Phase 1: Write all files to temporary locations
	tempFiles := make([]string, 0, len(files))
	totalSize := int64(0)

	for _, file := range files {
		fullPath := filepath.Join(l.basePath, file.Key)
		tempPath := fullPath + ".tmp"

		// Create directory structure
		dir := filepath.Dir(fullPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			// Cleanup: remove all temp files created so far
			l.cleanupTempFiles(tempFiles)
			l.metrics.ErrorCount++
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}

		// Create temporary file
		dstFile, err := os.Create(tempPath)
		if err != nil {
			l.cleanupTempFiles(tempFiles)
			l.metrics.ErrorCount++
			return fmt.Errorf("failed to create temp file %s: %w", tempPath, err)
		}

		// Copy data
		written, err := io.Copy(dstFile, file.Data)
		dstFile.Close()

		if err != nil {
			os.Remove(tempPath)
			l.cleanupTempFiles(tempFiles)
			l.metrics.ErrorCount++
			return fmt.Errorf("failed to write data to temp file %s: %w", tempPath, err)
		}

		tempFiles = append(tempFiles, tempPath)
		totalSize += written
	}

	// Phase 2: Rename all temporary files to final locations
	for i, file := range files {
		fullPath := filepath.Join(l.basePath, file.Key)
		tempPath := tempFiles[i]

		if err := os.Rename(tempPath, fullPath); err != nil {
			// Note: Some temp files might have been renamed already
			// but we don't rollback successful renames for simplicity
			l.metrics.ErrorCount++
			return fmt.Errorf("failed to rename temp file %s to %s: %w", tempPath, fullPath, err)
		}

		l.metrics.PutOperations++
	}

	l.metrics.BytesWritten += totalSize

	l.logger.WithFields(logrus.Fields{
		"count": len(files),
		"size":  totalSize,
		"took":  time.Since(start),
	}).Debug("LocalBackend: PutBatch completed")

	return nil
}

// DeleteBatch removes multiple files
func (l *LocalBackend) DeleteBatch(ctx context.Context, keys []string) error {
	start := time.Now()
	defer func() {
		l.metrics.LastOperation = time.Now()
	}()

	var errors []string
	successCount := 0

	for _, key := range keys {
		fullPath := filepath.Join(l.basePath, key)

		err := os.Remove(fullPath)
		if err != nil {
			if !os.IsNotExist(err) {
				errors = append(errors, fmt.Sprintf("failed to delete %s: %v", key, err))
				l.metrics.ErrorCount++
			}
		} else {
			successCount++
			l.metrics.DeleteOperations++
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch delete had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	l.logger.WithFields(logrus.Fields{
		"requested": len(keys),
		"deleted":   successCount,
		"took":      time.Since(start),
	}).Debug("LocalBackend: DeleteBatch completed")

	return nil
}

// GetSize returns the size of an object
func (l *LocalBackend) GetSize(ctx context.Context, key string) (int64, error) {
	fullPath := filepath.Join(l.basePath, key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrObjectNotFound
		}
		l.metrics.ErrorCount++
		return 0, fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}

	return info.Size(), nil
}

// GetLastModified returns the last modified time of an object
func (l *LocalBackend) GetLastModified(ctx context.Context, key string) (time.Time, error) {
	fullPath := filepath.Join(l.basePath, key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return time.Time{}, ErrObjectNotFound
		}
		l.metrics.ErrorCount++
		return time.Time{}, fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}

	return info.ModTime(), nil
}

// Close closes the backend
func (l *LocalBackend) Close() error {
	// Local backend doesn't need explicit cleanup
	l.logger.Info("LocalBackend: Closed")
	return nil
}

// Health checks the health of the backend
func (l *LocalBackend) Health(ctx context.Context) error {
	// Check if base directory is accessible
	info, err := os.Stat(l.basePath)
	if err != nil {
		return fmt.Errorf("base directory not accessible: %w", err)
	}

	if !info.IsDir() {
		return fmt.Errorf("base path is not a directory: %s", l.basePath)
	}

	// Test write access
	testFile := filepath.Join(l.basePath, ".health-check")
	file, err := os.Create(testFile)
	if err != nil {
		return fmt.Errorf("no write access to base directory: %w", err)
	}
	file.Close()
	os.Remove(testFile)

	return nil
}

// Type returns the backend type
func (l *LocalBackend) Type() string {
	return "local"
}

// GetMetrics returns storage metrics
func (l *LocalBackend) GetMetrics() Metrics {
	return l.metrics
}

// cleanupTempFiles removes temporary files created during batch operations
func (l *LocalBackend) cleanupTempFiles(tempFiles []string) {
	for _, tempFile := range tempFiles {
		if err := os.Remove(tempFile); err != nil && !os.IsNotExist(err) {
			l.logger.WithError(err).WithField("file", tempFile).Warn("Failed to cleanup temp file")
		}
	}
}
