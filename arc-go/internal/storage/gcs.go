package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSBackend implements the Backend interface for Google Cloud Storage
type GCSBackend struct {
	client  *storage.Client
	config  GCSConfig
	logger  *logrus.Logger
	metrics Metrics
}

// GCSConfig holds GCS configuration
type GCSConfig struct {
	Bucket          string `toml:"bucket"`
	ProjectID       string `toml:"project_id"`
	CredentialsFile string `toml:"credentials_file"`
	Database        string `toml:"database"`
	JSONKey         string `toml:"json_key,omitempty"`
}

// NewGCSBackend creates a new GCS storage backend
func NewGCSBackend(config GCSConfig, logger *logrus.Logger) (*GCSBackend, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// Validate configuration
	if config.Bucket == "" {
		return nil, fmt.Errorf("gcs bucket is required")
	}
	if config.ProjectID == "" {
		return nil, fmt.Errorf("gcs project_id is required")
	}
	if config.Database == "" {
		config.Database = "default"
	}

	// Create client options
	var opts []option.ClientOption

	// Try credentials file first
	if config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsFile))
	}

	// Try JSON key if provided
	if config.JSONKey != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(config.JSONKey)))
	}

	// Create client
	client, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	backend := &GCSBackend{
		client: client,
		config: config,
		logger: logger,
	}

	// Test bucket access
	bucket := client.Bucket(config.Bucket)
	_, err = bucket.Attrs(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to access GCS bucket %s: %w", config.Bucket, err)
	}

	logger.WithFields(logrus.Fields{
		"bucket":  config.Bucket,
		"project": config.ProjectID,
		"db":      config.Database,
	}).Info("GCS backend initialized")

	return backend, nil
}

// Put stores data at the given key
func (g *GCSBackend) Put(ctx context.Context, key string, data io.Reader, size int64) error {
	start := time.Now()
	defer func() {
		g.metrics.PutOperations++
		g.metrics.BytesWritten += size
		g.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

	g.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"size":       size,
		"bucket":     g.config.Bucket,
	}).Debug("GCS: Putting object")

	// Upload object
	bucket := g.client.Bucket(g.config.Bucket)
	obj := bucket.Object(objectKey)
	writer := obj.NewWriter(ctx)

	// Copy data
	written, err := io.Copy(writer, data)
	if err != nil {
		g.metrics.ErrorCount++
		return fmt.Errorf("failed to write object %s: %w", objectKey, err)
	}

	// Close writer to finalize upload
	if err := writer.Close(); err != nil {
		g.metrics.ErrorCount++
		return fmt.Errorf("failed to close writer for object %s: %w", objectKey, err)
	}

	g.logger.WithFields(logrus.Fields{
		"key":  key,
		"size": written,
		"took": time.Since(start),
	}).Debug("GCS: Put completed")

	return nil
}

// Get retrieves data from the given key
func (g *GCSBackend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	defer func() {
		g.metrics.GetOperations++
		g.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

	g.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"bucket":     g.config.Bucket,
	}).Debug("GCS: Getting object")

	// Get object
	bucket := g.client.Bucket(g.config.Bucket)
	obj := bucket.Object(objectKey)
	reader, err := obj.NewReader(ctx)

	if err != nil {
		g.metrics.ErrorCount++
		if err == storage.ErrObjectNotExist {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("failed to get object %s: %w", objectKey, err)
	}

	// Get object size for metrics
	attrs, err := obj.Attrs(ctx)
	if err == nil {
		g.metrics.BytesRead += attrs.Size
	}

	g.logger.WithFields(logrus.Fields{
		"key":  key,
		"took": time.Since(start),
	}).Debug("GCS: Get completed")

	return reader, nil
}

// Delete removes data at the given key
func (g *GCSBackend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer func() {
		g.metrics.DeleteOperations++
		g.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

	g.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"bucket":     g.config.Bucket,
	}).Debug("GCS: Deleting object")

	// Delete object
	bucket := g.client.Bucket(g.config.Bucket)
	obj := bucket.Object(objectKey)

	if err := obj.Delete(ctx); err != nil {
		g.metrics.ErrorCount++
		if err == storage.ErrObjectNotExist {
			return ErrObjectNotFound
		}
		return fmt.Errorf("failed to delete object %s: %w", objectKey, err)
	}

	g.logger.WithFields(logrus.Fields{
		"key":  key,
		"took": time.Since(start),
	}).Debug("GCS: Delete completed")

	return nil
}

// Exists checks if a key exists
func (g *GCSBackend) Exists(ctx context.Context, key string) (bool, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

	bucket := g.client.Bucket(g.config.Bucket)
	obj := bucket.Object(objectKey)

	_, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		g.metrics.ErrorCount++
		return false, fmt.Errorf("failed to get object attrs %s: %w", objectKey, err)
	}

	return true, nil
}

// List returns objects with the given prefix
func (g *GCSBackend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	start := time.Now()
	defer func() {
		g.metrics.ListOperations++
		g.metrics.LastOperation = time.Now()
	}()

	// Construct prefix with database
	listPrefix := fmt.Sprintf("%s/%s", g.config.Database, prefix)

	g.logger.WithFields(logrus.Fields{
		"prefix":      prefix,
		"list_prefix": listPrefix,
		"bucket":      g.config.Bucket,
	}).Debug("GCS: Listing objects")

	var objects []ObjectInfo

	// List objects
	bucket := g.client.Bucket(g.config.Bucket)
	it := bucket.Objects(ctx, &storage.Query{
		Prefix: listPrefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			g.metrics.ErrorCount++
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		// Remove database prefix from key
		key := strings.TrimPrefix(attrs.Name, g.config.Database+"/")

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         attrs.Size,
			LastModified: attrs.Updated,
			ETag:         base64.StdEncoding.EncodeToString(attrs.MD5),
			ContentType:  attrs.ContentType,
		})
	}

	g.logger.WithFields(logrus.Fields{
		"prefix": prefix,
		"count":  len(objects),
		"took":   time.Since(start),
	}).Debug("GCS: List completed")

	return objects, nil
}

// ListWithRecursive returns objects with the given prefix, optionally recursive
func (g *GCSBackend) ListWithRecursive(ctx context.Context, prefix string, recursive bool) ([]ObjectInfo, error) {
	// GCS ListObjects is always recursive, so we can use the same implementation
	return g.List(ctx, prefix)
}

// PutBatch stores multiple files atomically
func (g *GCSBackend) PutBatch(ctx context.Context, files []BatchFile) error {
	start := time.Now()
	defer func() {
		g.metrics.LastOperation = time.Now()
	}()

	if len(files) == 0 {
		return nil
	}

	g.logger.WithFields(logrus.Fields{
		"count":  len(files),
		"bucket": g.config.Bucket,
	}).Debug("GCS: Batch put")

	// For GCS, we'll upload files sequentially for simplicity
	totalSize := int64(0)
	successCount := 0

	for _, file := range files {
		// Read all data to get size
		data, err := io.ReadAll(file.Data)
		if err != nil {
			g.logger.WithError(err).WithField("key", file.Key).Error("Failed to read file data")
			continue
		}

		// Construct object key with database prefix
		objectKey := fmt.Sprintf("%s/%s", g.config.Database, file.Key)

		// Upload object
		bucket := g.client.Bucket(g.config.Bucket)
		obj := bucket.Object(objectKey)
		writer := obj.NewWriter(ctx)

		written, err := io.Copy(writer, bytes.NewReader(data))
		if err != nil {
			g.logger.WithError(err).WithField("key", file.Key).Error("Failed to write file data")
			continue
		}

		if err := writer.Close(); err != nil {
			g.logger.WithError(err).WithField("key", file.Key).Error("Failed to close writer")
			continue
		}

		totalSize += int64(written)
		successCount++
	}

	g.metrics.PutOperations += int64(successCount)
	g.metrics.BytesWritten += totalSize

	g.logger.WithFields(logrus.Fields{
		"requested": len(files),
		"success":   successCount,
		"size":      totalSize,
		"took":      time.Since(start),
	}).Debug("GCS: Batch put completed")

	if successCount == 0 {
		return fmt.Errorf("no files were successfully uploaded")
	}

	return nil
}

// DeleteBatch removes multiple files
func (g *GCSBackend) DeleteBatch(ctx context.Context, keys []string) error {
	start := time.Now()
	defer func() {
		g.metrics.LastOperation = time.Now()
	}()

	if len(keys) == 0 {
		return nil
	}

	g.logger.WithFields(logrus.Fields{
		"count":  len(keys),
		"bucket": g.config.Bucket,
	}).Debug("GCS: Batch delete")

	// For GCS, we'll delete files sequentially
	successCount := 0

	for _, key := range keys {
		// Construct object key with database prefix
		objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

		bucket := g.client.Bucket(g.config.Bucket)
		obj := bucket.Object(objectKey)

		if err := obj.Delete(ctx); err != nil {
			if err != storage.ErrObjectNotExist {
				g.logger.WithError(err).WithField("key", key).Error("Failed to delete file")
			}
			continue
		}

		successCount++
	}

	g.metrics.DeleteOperations += int64(successCount)

	g.logger.WithFields(logrus.Fields{
		"requested": len(keys),
		"deleted":   successCount,
		"took":      time.Since(start),
	}).Debug("GCS: Batch delete completed")

	return nil
}

// GetSize returns the size of an object
func (g *GCSBackend) GetSize(ctx context.Context, key string) (int64, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

	bucket := g.client.Bucket(g.config.Bucket)
	obj := bucket.Object(objectKey)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return 0, ErrObjectNotFound
		}
		g.metrics.ErrorCount++
		return 0, fmt.Errorf("failed to get object attrs %s: %w", objectKey, err)
	}

	return attrs.Size, nil
}

// GetLastModified returns the last modified time of an object
func (g *GCSBackend) GetLastModified(ctx context.Context, key string) (time.Time, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", g.config.Database, key)

	bucket := g.client.Bucket(g.config.Bucket)
	obj := bucket.Object(objectKey)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return time.Time{}, ErrObjectNotFound
		}
		g.metrics.ErrorCount++
		return time.Time{}, fmt.Errorf("failed to get object attrs %s: %w", objectKey, err)
	}

	return attrs.Updated, nil
}

// Close closes the backend
func (g *GCSBackend) Close() error {
	g.logger.Info("GCS backend: Closed")
	return nil
}

// Health checks the health of the backend
func (g *GCSBackend) Health(ctx context.Context) error {
	// Test bucket access
	bucket := g.client.Bucket(g.config.Bucket)
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to access GCS bucket %s: %w", g.config.Bucket, err)
	}

	return nil
}

// Type returns the backend type
func (g *GCSBackend) Type() string {
	return "gcs"
}

// GetMetrics returns storage metrics
func (g *GCSBackend) GetMetrics() Metrics {
	return g.metrics
}

// GetClient returns the underlying GCS client (for advanced usage)
func (g *GCSBackend) GetClient() *storage.Client {
	return g.client
}

// GetConfig returns the GCS configuration
func (g *GCSBackend) GetConfig() GCSConfig {
	return g.config
}
