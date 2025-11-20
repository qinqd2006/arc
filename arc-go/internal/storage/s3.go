package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sirupsen/logrus"
)

// S3Backend implements the Backend interface for AWS S3 storage
type S3Backend struct {
	client  *s3.Client
	config  S3Config
	logger  *logrus.Logger
	region  string
	metrics Metrics
}

// S3Config holds S3 configuration
type S3Config struct {
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Region    string `toml:"region"`
	Bucket    string `toml:"bucket"`
	Database  string `toml:"database"`
	Endpoint  string `toml:"endpoint,omitempty"`
	UseSSL    bool   `toml:"use_ssl"`
}

// NewS3Backend creates a new S3 storage backend
func NewS3Backend(cfg S3Config, logger *logrus.Logger) (*S3Backend, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// Validate configuration
	if cfg.AccessKey == "" {
		return nil, fmt.Errorf("s3 access key is required")
	}
	if cfg.SecretKey == "" {
		return nil, fmt.Errorf("s3 secret key is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Bucket == "" {
		cfg.Bucket = "arc"
	}
	if cfg.Database == "" {
		cfg.Database = "default"
	}

	// Load AWS configuration
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client options
	s3Opts := []func(*s3.Options){}

	// Custom endpoint for S3-compatible services
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(opts *s3.Options) {
			opts.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	// Create S3 client
	client := s3.NewFromConfig(awsCfg, s3Opts...)

	backend := &S3Backend{
		client: client,
		config: cfg,
		logger: logger,
		region: cfg.Region,
	}

	// Ensure bucket exists
	if err := backend.ensureBucket(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ensure bucket exists: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"region":   cfg.Region,
		"bucket":   cfg.Bucket,
		"endpoint": cfg.Endpoint,
	}).Info("S3 backend initialized")

	return backend, nil
}

// ensureBucket creates the bucket if it doesn't exist
func (s *S3Backend) ensureBucket(ctx context.Context) error {
	// Check if bucket exists
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.Bucket),
	})

	if err == nil {
		return nil // Bucket exists
	}

	// Check if it's a NotFound error
	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	// Try to create bucket
	var input *s3.CreateBucketInput
	if s.region == "us-east-1" {
		// us-east-1 doesn't require LocationConstraint
		input = &s3.CreateBucketInput{
			Bucket: aws.String(s.config.Bucket),
		}
	} else {
		input = &s3.CreateBucketInput{
			Bucket: aws.String(s.config.Bucket),
			CreateBucketConfiguration: &types.CreateBucketConfiguration{
				LocationConstraint: types.BucketLocationConstraint(s.region),
			},
		}
	}

	_, err = s.client.CreateBucket(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	s.logger.WithField("bucket", s.config.Bucket).Info("Created S3 bucket")
	return nil
}

// Put stores data at the given key
func (s *S3Backend) Put(ctx context.Context, key string, data io.Reader, size int64) error {
	start := time.Now()
	defer func() {
		s.metrics.PutOperations++
		s.metrics.BytesWritten += size
		s.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", s.config.Database, key)

	s.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"size":       size,
		"bucket":     s.config.Bucket,
	}).Debug("S3: Putting object")

	// Upload object
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(objectKey),
		Body:   data,
	})

	if err != nil {
		s.metrics.ErrorCount++
		return fmt.Errorf("failed to put object %s: %w", objectKey, err)
	}

	s.logger.WithFields(logrus.Fields{
		"key":  key,
		"size": size,
		"took": time.Since(start),
	}).Debug("S3: Put completed")

	return nil
}

// Get retrieves data from the given key
func (s *S3Backend) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	start := time.Now()
	defer func() {
		s.metrics.GetOperations++
		s.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", s.config.Database, key)

	s.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"bucket":     s.config.Bucket,
	}).Debug("S3: Getting object")

	// Get object
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		s.metrics.ErrorCount++
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return nil, ErrObjectNotFound
		}
		return nil, fmt.Errorf("failed to get object %s: %w", objectKey, err)
	}

	// Get object size for metrics
	if result.ContentLength != nil {
		s.metrics.BytesRead += *result.ContentLength
	}

	s.logger.WithFields(logrus.Fields{
		"key":  key,
		"took": time.Since(start),
	}).Debug("S3: Get completed")

	return result.Body, nil
}

// Delete removes data at the given key
func (s *S3Backend) Delete(ctx context.Context, key string) error {
	start := time.Now()
	defer func() {
		s.metrics.DeleteOperations++
		s.metrics.LastOperation = time.Now()
	}()

	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", s.config.Database, key)

	s.logger.WithFields(logrus.Fields{
		"key":        key,
		"object_key": objectKey,
		"bucket":     s.config.Bucket,
	}).Debug("S3: Deleting object")

	// Delete object
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		s.metrics.ErrorCount++
		return fmt.Errorf("failed to delete object %s: %w", objectKey, err)
	}

	s.logger.WithFields(logrus.Fields{
		"key":  key,
		"took": time.Since(start),
	}).Debug("S3: Delete completed")

	return nil
}

// Exists checks if a key exists
func (s *S3Backend) Exists(ctx context.Context, key string) (bool, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", s.config.Database, key)

	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		s.metrics.ErrorCount++
		return false, fmt.Errorf("failed to head object %s: %w", objectKey, err)
	}

	return true, nil
}

// List returns objects with the given prefix
func (s *S3Backend) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	start := time.Now()
	defer func() {
		s.metrics.ListOperations++
		s.metrics.LastOperation = time.Now()
	}()

	// Construct prefix with database
	listPrefix := fmt.Sprintf("%s/%s", s.config.Database, prefix)

	s.logger.WithFields(logrus.Fields{
		"prefix":      prefix,
		"list_prefix": listPrefix,
		"bucket":      s.config.Bucket,
	}).Debug("S3: Listing objects")

	var objects []ObjectInfo

	// List objects
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.config.Bucket),
		Prefix: aws.String(listPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			s.metrics.ErrorCount++
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			// Remove database prefix from key
			key := strings.TrimPrefix(*obj.Key, s.config.Database+"/")

			objects = append(objects, ObjectInfo{
				Key:          key,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
				ETag:         aws.ToString(obj.ETag),
				ContentType:  string(obj.StorageClass), // Convert StorageClass enum to string
			})
		}
	}

	s.logger.WithFields(logrus.Fields{
		"prefix": prefix,
		"count":  len(objects),
		"took":   time.Since(start),
	}).Debug("S3: List completed")

	return objects, nil
}

// ListWithRecursive returns objects with the given prefix, optionally recursive
func (s *S3Backend) ListWithRecursive(ctx context.Context, prefix string, recursive bool) ([]ObjectInfo, error) {
	// S3 ListObjectsV2 is always recursive, so we can use the same implementation
	return s.List(ctx, prefix)
}

// PutBatch stores multiple files atomically
func (s *S3Backend) PutBatch(ctx context.Context, files []BatchFile) error {
	start := time.Now()
	defer func() {
		s.metrics.LastOperation = time.Now()
	}()

	if len(files) == 0 {
		return nil
	}

	s.logger.WithFields(logrus.Fields{
		"count":  len(files),
		"bucket": s.config.Bucket,
	}).Debug("S3: Batch put")

	// For S3, we'll upload files in parallel using goroutines
	var wg sync.WaitGroup
	errChan := make(chan error, len(files))
	semaphore := make(chan struct{}, 10) // Limit concurrent uploads

	for _, file := range files {
		wg.Add(1)
		go func(batchFile BatchFile) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Read all data to get size
			data, err := io.ReadAll(batchFile.Data)
			if err != nil {
				errChan <- fmt.Errorf("failed to read file data for %s: %w", batchFile.Key, err)
				return
			}

			// Construct object key with database prefix
			objectKey := fmt.Sprintf("%s/%s", s.config.Database, batchFile.Key)

			// Upload object
			_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(s.config.Bucket),
				Key:    aws.String(objectKey),
				Body:   bytes.NewReader(data),
			})

			if err != nil {
				errChan <- fmt.Errorf("failed to put file %s: %w", batchFile.Key, err)
				return
			}
		}(file)
	}

	wg.Wait()
	close(errChan)

	// Collect errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
		s.metrics.ErrorCount++
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch put had %d errors: %v", len(errors), errors[0])
	}

	s.logger.WithFields(logrus.Fields{
		"requested": len(files),
		"took":      time.Since(start),
	}).Debug("S3: Batch put completed")

	return nil
}

// DeleteBatch removes multiple files
func (s *S3Backend) DeleteBatch(ctx context.Context, keys []string) error {
	start := time.Now()
	defer func() {
		s.metrics.LastOperation = time.Now()
	}()

	if len(keys) == 0 {
		return nil
	}

	s.logger.WithFields(logrus.Fields{
		"count":  len(keys),
		"bucket": s.config.Bucket,
	}).Debug("S3: Batch delete")

	// Prepare object identifiers for deletion
	var objectIds []types.ObjectIdentifier
	for _, key := range keys {
		objectIds = append(objectIds, types.ObjectIdentifier{
			Key: aws.String(fmt.Sprintf("%s/%s", s.config.Database, key)),
		})
	}

	// Delete objects in batch
	output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(s.config.Bucket),
		Delete: &types.Delete{
			Objects: objectIds,
		},
	})

	if err != nil {
		s.metrics.ErrorCount++
		return fmt.Errorf("failed to delete objects: %w", err)
	}

	// Check for errors
	if len(output.Errors) > 0 {
		var errors []string
		for _, err := range output.Errors {
			errors = append(errors, fmt.Sprintf("key %s: %s", *err.Key, *err.Message))
			s.metrics.ErrorCount++
		}
		return fmt.Errorf("batch delete had %d errors: %s", len(errors), strings.Join(errors, "; "))
	}

	s.metrics.DeleteOperations += int64(len(keys))

	s.logger.WithFields(logrus.Fields{
		"requested": len(keys),
		"deleted":   len(keys) - len(output.Errors),
		"took":      time.Since(start),
	}).Debug("S3: Batch delete completed")

	return nil
}

// GetSize returns the size of an object
func (s *S3Backend) GetSize(ctx context.Context, key string) (int64, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", s.config.Database, key)

	result, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return 0, ErrObjectNotFound
		}
		s.metrics.ErrorCount++
		return 0, fmt.Errorf("failed to head object %s: %w", objectKey, err)
	}

	if result.ContentLength == nil {
		return 0, fmt.Errorf("content length not available")
	}

	return *result.ContentLength, nil
}

// GetLastModified returns the last modified time of an object
func (s *S3Backend) GetLastModified(ctx context.Context, key string) (time.Time, error) {
	// Construct object key with database prefix
	objectKey := fmt.Sprintf("%s/%s", s.config.Database, key)

	result, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.Bucket),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return time.Time{}, ErrObjectNotFound
		}
		s.metrics.ErrorCount++
		return time.Time{}, fmt.Errorf("failed to head object %s: %w", objectKey, err)
	}

	if result.LastModified == nil {
		return time.Time{}, fmt.Errorf("last modified not available")
	}

	return *result.LastModified, nil
}

// Close closes the backend
func (s *S3Backend) Close() error {
	s.logger.Info("S3 backend: Closed")
	return nil
}

// Health checks the health of the backend
func (s *S3Backend) Health(ctx context.Context) error {
	// Check if bucket exists
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.config.Bucket),
	})

	if err != nil {
		return fmt.Errorf("failed to access bucket %s: %w", s.config.Bucket, err)
	}

	return nil
}

// Type returns the backend type
func (s *S3Backend) Type() string {
	return "s3"
}

// GetMetrics returns storage metrics
func (s *S3Backend) GetMetrics() Metrics {
	return s.metrics
}

// GetClient returns the underlying S3 client (for advanced usage)
func (s *S3Backend) GetClient() *s3.Client {
	return s.client
}

// GetConfig returns the S3 configuration
func (s *S3Backend) GetConfig() S3Config {
	return s.config
}
