package storage

import (
	"context"
	"io"
	"time"
)

// Backend defines the storage backend interface
type Backend interface {
	// File operations
	Put(ctx context.Context, key string, data io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)

	// Directory operations
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	ListWithRecursive(ctx context.Context, prefix string, recursive bool) ([]ObjectInfo, error)

	// Batch operations
	PutBatch(ctx context.Context, files []BatchFile) error
	DeleteBatch(ctx context.Context, keys []string) error

	// Metadata operations
	GetSize(ctx context.Context, key string) (int64, error)
	GetLastModified(ctx context.Context, key string) (time.Time, error)

	// Backend operations
	Close() error
	Health(ctx context.Context) error
	Type() string
}

// ObjectInfo represents information about a storage object
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
}

// BatchFile represents a file for batch operations
type BatchFile struct {
	Key  string
	Data io.Reader
	Size int64
	Etag string
}

// PathBuilder builds storage paths for measurements
type PathBuilder interface {
	// BuildMeasurementPath creates a path for a measurement
	BuildMeasurementPath(database, measurement string, timestamp time.Time) string

	// BuildHourlyPath creates a path for hourly partitioning
	BuildHourlyPath(database, measurement string, year, month, day, hour int) string

	// BuildWALPath creates a path for WAL files
	BuildWALPath(database, workerID string) string

	// BuildCompactionPath creates a path for compaction temp files
	BuildCompactionPath(database, measurement string, timestamp time.Time) string

	// BuildParquetFileName creates a filename for Parquet files
	BuildParquetFileName(measurement string, timestamp time.Time) string

	// BuildDatabasePrefix returns the database-level prefix
	BuildDatabasePrefix(database string) string

	// BuildMeasurementPrefix returns the measurement-level prefix
	BuildMeasurementPrefix(database, measurement string) string

	// IsSystemPath checks if a path is a system path
	IsSystemPath(path string) bool

	// ParsePath parses a path into components
	ParsePath(path string) (database, measurement string, year, month, day, hour int, err error)

	// ParseTimeFromPath extracts the time from a path
	ParseTimeFromPath(path string) (time.Time, error)
}

// StorageConfig holds storage configuration
type StorageConfig struct {
	Backend string
	Config  interface{}
}

// Metrics represents storage metrics
type Metrics struct {
	PutOperations    int64
	GetOperations    int64
	DeleteOperations int64
	ListOperations   int64
	BytesWritten     int64
	BytesRead        int64
	ErrorCount       int64
	LastOperation    time.Time
}
