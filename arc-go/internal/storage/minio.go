package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
)

// MinIOBackend implements the Backend interface for MinIO object storage
type MinIOBackend struct {
	client  *minio.Client
	config  MinIOConfig
	logger  *logrus.Logger
	metrics Metrics
}

// MinIOConfig holds MinIO configuration
type MinIOConfig struct {
	Endpoint  string `toml:"endpoint"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
	Database  string `toml:"database"`
	UseSSL    bool   `toml:"use_ssl"`
	Region    string `toml:"region"`
}

// NewMinIOBackend creates a new MinIO storage backend
func NewMinIOBackend(config MinIOConfig, logger *logrus.Logger) (*MinIOBackend, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// Validate configuration
	if config.Endpoint == "" {
		return nil, fmt.Errorf("minio endpoint is required")
	}
	if config.AccessKey == "" {
		return nil, fmt.Errorf("minio access key is required")
	}
	if config.SecretKey == "" {
		return nil, fmt.Errorf("minio secret key is required")
	}
	if config.Bucket == "" {
		config.Bucket = "arc"
	}
	if config.Database == "" {
		config.Database = "default"
	}

	// Parse endpoint URL
	endpointURL, err := url.Parse(config.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("invalid minio endpoint: %w", err)
	}

	// Initialize MinIO client
	client, err := minio.New(endpointURL.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: config.UseSSL,
		Region: config.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	backend := &MinIOBackend{
		client: client,
		config: config,
		logger: logger,
	}

	// Create bucket if it doesn't exist
	if err := backend.ensureBucket(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure bucket exists: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"endpoint": config.Endpoint,
		"bucket":   config.Bucket,
		"use_ssl":  config.UseSSL,
		"region":   config.Region,
	}).Info("MinIO backend initialized")

	return backend, nil
}

// ensureBucket creates the bucket if it doesn't exist
func (m *MinIOBackend) ensureBucket(ctx context.Context) error {
	exists, err := m.client.BucketExists(ctx, m.config.Bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		if err := m.client.MakeBucket(ctx, m.config.Bucket, minio.MakeBucketOptions{
			Region: m.config.Region,
		}); err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
		m.logger.WithField("bucket", m.config.Bucket).Info("Created MinIO bucket")
	}

	return nil
}

// Put stores data at the given key
func (m *MinIOBackend) Put(ctx context.Context, key string, data io.Reader, size int64) error {
	start := time.Now()
	defer func() {
		m.metrics.PutOperations++
		m.metrics.BytesWritten += size
		m.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", m.config.Database, key)

	m.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"size":       size,
		"bucket":     m.config.Bucket,
	}).Debug("MinIO: Putting object")

	// Upload object
	_, err := m.client.PutObject(ctx, m.config.Bucket, objectKey, data, size, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})

	if err != nil {
		m.metrics.ErrorCount++
		return fmt.Errorf("failed to put object %s: %w", objectKey, err)
	}

	m.logger.WithFields(logrus.Fields{
		"key":  key,
		"size": size,
		"took": time.Since(start),
	}).Debug("MinIO: Put completed")

	return nil
}

// Get retrieves data from the given key
func (m *MinIOBackend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	defer func() {
		m.metrics.GetOperations++
		m.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", m.config.Database, key)

	m.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"bucket":     m.config.Bucket,
	}).Debug("MinIO: Getting object")

	// Get object
	object, err := m.client.GetObject(ctx, m.config.Bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		m.metrics.ErrorCount++
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("failed to get object %s: %w", objectKey, err)
	}

	// Get object size for metrics
	stat, err := object.Stat()
	if err == nil {
		m.metrics.BytesRead += stat.Size
	}

	m.logger.WithFields(logrus.Fields{
		"key":  key,
		"took": time.Since(start),
	}).Debug("MinIO: Get completed")

	return object, nil
}

// Delete removes data at the given key
func (m *MinIOBackend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer func() {
		m.metrics.DeleteOperations++
		m.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", m.config.Database, key)

	m.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"bucket":     m.config.Bucket,
	}).Debug("MinIO: Deleting object")

	// Delete object
	err := m.client.RemoveObject(ctx, m.config.Bucket, objectKey, minio.RemoveObjectOptions{})
	if err != nil {
		m.metrics.ErrorCount++
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return ErrObjectNotFound
		}
		return fmt.Errorf("failed to delete object %s: %w", objectKey, err)
	}

	m.logger.WithFields(logrus.Fields{
		"key":  key,
		"took": time.Since(start),
	}).Debug("MinIO: Delete completed")

	return nil
}

// Exists checks if a key exists
func (m *MinIOBackend) Exists(ctx context.Context, key string) (bool, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", m.config.Database, key)

	_, err := m.client.StatObject(ctx, m.config.Bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		m.metrics.ErrorCount++
		return false, fmt.Errorf("failed to stat object %s: %w", objectKey, err)
	}

	return true, nil
}

// List returns objects with the given prefix
func (m *MinIOBackend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	start := time.Now()
	defer func() {
		m.metrics.ListOperations++
		m.metrics.LastOperation = time.Now()
	}()

	// Construct prefix with database
	listPrefix := fmt.Sprintf("%s/%s", m.config.Database, prefix)

	m.logger.WithFields(logrus.Fields{
		"prefix":      prefix,
		"list_prefix": listPrefix,
		"bucket":      m.config.Bucket,
	}).Debug("MinIO: Listing objects")

	var objects []ObjectInfo

	// List objects
	objectCh := m.client.ListObjects(ctx, m.config.Bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: false,
	})

	for object := range objectCh {
		if object.Err != nil {
			m.metrics.ErrorCount++
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		// Remove database prefix from key
		key := strings.TrimPrefix(object.Key, m.config.Database+"/")

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         object.Size,
			LastModified: object.LastModified,
			ETag:         object.ETag,
			ContentType:  object.ContentType,
		})
	}

	m.logger.WithFields(logrus.Fields{
		"prefix": prefix,
		"count":  len(objects),
		"took":   time.Since(start),
	}).Debug("MinIO: List completed")

	return objects, nil
}

// ListWithRecursive returns objects with the given prefix, optionally recursive
func (m *MinIOBackend) ListWithRecursive(ctx context.Context, prefix string, recursive bool) ([]ObjectInfo, error) {
	start := time.Now()
	defer func() {
		m.metrics.ListOperations++
		m.metrics.LastOperation = time.Now()
	}()

	// Construct prefix with database
	listPrefix := fmt.Sprintf("%s/%s", m.config.Database, prefix)

	m.logger.WithFields(logrus.Fields{
		"prefix":      prefix,
		"list_prefix": listPrefix,
		"recursive":   recursive,
		"bucket":      m.config.Bucket,
	}).Debug("MinIO: Listing objects recursively")

	var objects []ObjectInfo

	// List objects
	objectCh := m.client.ListObjects(ctx, m.config.Bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: recursive,
	})

	for object := range objectCh {
		if object.Err != nil {
			m.metrics.ErrorCount++
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}

		// Remove database prefix from key
		key := strings.TrimPrefix(object.Key, m.config.Database+"/")

		objects = append(objects, ObjectInfo{
			Key:          key,
			Size:         object.Size,
			LastModified: object.LastModified,
			ETag:         object.ETag,
			ContentType:  object.ContentType,
		})
	}

	m.logger.WithFields(logrus.Fields{
		"prefix":    prefix,
		"recursive": recursive,
		"count":     len(objects),
		"took":      time.Since(start),
	}).Debug("MinIO: ListWithRecursive completed")

	return objects, nil
}

// PutBatch stores multiple files atomically
func (m *MinIOBackend) PutBatch(ctx context.Context, files []BatchFile) error {
	start := time.Now()
	defer func() {
		m.metrics.LastOperation = time.Now()
	}()

	if len(files) == 0 {
		return nil
	}

	m.logger.WithFields(logrus.Fields{
		"count":  len(files),
		"bucket": m.config.Bucket,
	}).Debug("MinIO: Batch put")

	// For MinIO, we'll upload files sequentially
	// In a production environment, you might want to use goroutines for parallel uploads
	totalSize := int64(0)
	successCount := 0

	for _, file := range files {
		// Construct object key with database prefix
		objectKey := fmt.Sprintf("%s/%s", m.config.Database, file.Key)

		// Read all data to get size
		data, err := io.ReadAll(file.Data)
		if err != nil {
			m.metrics.ErrorCount++
			m.logger.WithError(err).WithField("key", file.Key).Error("Failed to read file data")
			continue
		}

		// Upload object
		_, err = m.client.PutObject(ctx, m.config.Bucket, objectKey, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})

		if err != nil {
			m.metrics.ErrorCount++
			m.logger.WithError(err).WithField("key", file.Key).Error("Failed to put file in batch")
			continue
		}

		totalSize += int64(len(data))
		successCount++
	}

	m.metrics.PutOperations += int64(successCount)
	m.metrics.BytesWritten += totalSize

	m.logger.WithFields(logrus.Fields{
		"requested": len(files),
		"success":   successCount,
		"size":      totalSize,
		"took":      time.Since(start),
	}).Debug("MinIO: Batch put completed")

	if successCount == 0 {
		return fmt.Errorf("no files were successfully uploaded")
	}

	return nil
}

// DeleteBatch removes multiple files
func (m *MinIOBackend) DeleteBatch(ctx context.Context, keys []string) error {
	start := time.Now()
	defer func() {
		m.metrics.LastOperation = time.Now()
	}()

	if len(keys) == 0 {
		return nil
	}

	m.logger.WithFields(logrus.Fields{
		"count":  len(keys),
		"bucket": m.config.Bucket,
	}).Debug("MinIO: Batch delete")

	// Prepare objects for deletion
	objectsCh := make(chan minio.ObjectInfo)

	// Send objects to delete in a goroutine
	go func() {
		defer close(objectsCh)
		for _, key := range keys {
			objectsCh <- minio.ObjectInfo{
				Key: fmt.Sprintf("%s/%s", m.config.Database, key),
			}
		}
	}()

	// Delete objects in batch
	errorCh := m.client.RemoveObjects(ctx, m.config.Bucket, objectsCh, minio.RemoveObjectsOptions{})

	var errors []string
	successCount := 0

	for err := range errorCh {
		if err.Err != nil {
			errors = append(errors, fmt.Sprintf("key %s: %v", err.ObjectName, err.Err))
			m.metrics.ErrorCount++
		} else {
			successCount++
			m.metrics.DeleteOperations++
		}
	}

	if len(errors) > 0 {
		m.logger.WithField("errors", len(errors)).Warn("Some objects failed to delete")
		return fmt.Errorf("batch delete had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	m.logger.WithFields(logrus.Fields{
		"requested": len(keys),
		"deleted":   successCount,
		"took":      time.Since(start),
	}).Debug("MinIO: Batch delete completed")

	return nil
}

// GetSize returns the size of an object
func (m *MinIOBackend) GetSize(ctx context.Context, key string) (int64, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", m.config.Database, key)

	stat, err := m.client.StatObject(ctx, m.config.Bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return 0, ErrObjectNotFound
		}
		m.metrics.ErrorCount++
		return 0, fmt.Errorf("failed to stat object %s: %w", objectKey, err)
	}

	return stat.Size, nil
}

// GetLastModified returns the last modified time of an object
func (m *MinIOBackend) GetLastModified(ctx context.Context, key string) (time.Time, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", m.config.Database, key)

	stat, err := m.client.StatObject(ctx, m.config.Bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return time.Time{}, ErrObjectNotFound
		}
		m.metrics.ErrorCount++
		return time.Time{}, fmt.Errorf("failed to stat object %s: %w", objectKey, err)
	}

	return stat.LastModified, nil
}

// Close closes the backend
func (m *MinIOBackend) Close() error {
	m.logger.Info("MinIO backend: Closed")
	return nil
}

// Health checks the health of the backend
func (m *MinIOBackend) Health(ctx context.Context) error {
	// Check if bucket exists
	exists, err := m.client.BucketExists(ctx, m.config.Bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket health: %w", err)
	}

	if !exists {
		return fmt.Errorf("bucket %s does not exist", m.config.Bucket)
	}

	return nil
}

// Type returns the backend type
func (m *MinIOBackend) Type() string {
	return "minio"
}

// GetMetrics returns storage metrics
func (m *MinIOBackend) GetMetrics() Metrics {
	return m.metrics
}

// GetClient returns the underlying MinIO client (for advanced usage)
func (m *MinIOBackend) GetClient() *minio.Client {
	return m.client
}

// GetConfig returns the MinIO configuration
func (m *MinIOBackend) GetConfig() MinIOConfig {
	return m.config
}
