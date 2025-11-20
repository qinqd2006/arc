package compaction

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/sirupsen/logrus"
)

// Compactor handles the actual compaction of Parquet files
type Compactor struct {
	config      *config.CompactionConfig
	storage     storage.Backend
	pathBuilder storage.PathBuilder
	logger      *logrus.Logger
	pool        memory.Allocator
	mu          sync.RWMutex
	locks       map[string]*CompactionLock
}

// CompactionResult represents the result of a compaction operation
type CompactionResult struct {
	FilesCompacted      int64         `json:"files_compacted"`
	FilesCreated        int64         `json:"files_created"`
	BytesProcessed      int64         `json:"bytes_processed"`
	BytesWritten        int64         `json:"bytes_written"`
	PartitionsCompacted []string      `json:"partitions_compacted"`
	Duration            time.Duration `json:"duration"`
	Success             bool          `json:"success"`
	Error               string        `json:"error,omitempty"`
}

// CompactionLock represents a lock for a partition being compacted
type CompactionLock struct {
	Partition string    `json:"partition"`
	WorkerID  string    `json:"worker_id"`
	StartTime time.Time `json:"start_time"`
	ExpiresAt time.Time `json:"expires_at"`
}

// CompactionTask represents a compaction task for a partition
type CompactionTask struct {
	Database    string               `json:"database"`
	Measurement string               `json:"measurement"`
	Partition   string               `json:"partition"`
	Files       []storage.ObjectInfo `json:"files"`
	Priority    int                  `json:"priority"`
	CreatedAt   time.Time            `json:"created_at"`
}

// NewCompactor creates a new compactor
func NewCompactor(
	cfg *config.CompactionConfig,
	storage storage.Backend,
	pathBuilder storage.PathBuilder,
	logger *logrus.Logger,
) *Compactor {
	if logger == nil {
		logger = logrus.New()
	}

	return &Compactor{
		config:      cfg,
		storage:     storage,
		pathBuilder: pathBuilder,
		logger:      logger,
		pool:        memory.NewGoAllocator(),
		locks:       make(map[string]*CompactionLock),
	}
}

// Compact performs compaction on eligible partitions
func (c *Compactor) Compact(ctx context.Context) (*CompactionResult, error) {
	start := time.Now()

	c.logger.Info("Starting compaction process")

	// Find partitions that need compaction
	tasks, err := c.findCompactionTasks(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to find compaction tasks: %w", err)
	}

	if len(tasks) == 0 {
		c.logger.Info("No partitions require compaction")
		return &CompactionResult{
			Duration: time.Since(start),
			Success:  true,
		}, nil
	}

	c.logger.WithField("tasks_count", len(tasks)).Info("Found partitions requiring compaction")

	// Sort tasks by priority
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].Priority > tasks[j].Priority
	})

	// Execute compaction tasks
	result := &CompactionResult{
		PartitionsCompacted: make([]string, 0),
		Success:             true,
	}

	for _, task := range tasks {
		if err := c.compactPartition(ctx, task, result); err != nil {
			c.logger.WithError(err).WithFields(logrus.Fields{
				"database":    task.Database,
				"measurement": task.Measurement,
				"partition":   task.Partition,
			}).Error("Failed to compact partition")

			result.Success = false
			result.Error = err.Error()
			continue
		}

		// Check context for cancellation
		select {
		case <-ctx.Done():
			c.logger.Info("Compaction cancelled by context")
			result.Success = false
			result.Error = "compaction cancelled"
			return result, nil
		default:
		}
	}

	result.Duration = time.Since(start)

	c.logger.WithFields(logrus.Fields{
		"duration":             result.Duration,
		"files_compacted":      result.FilesCompacted,
		"files_created":        result.FilesCreated,
		"bytes_processed":      result.BytesProcessed,
		"bytes_written":        result.BytesWritten,
		"partitions_compacted": len(result.PartitionsCompacted),
		"success":              result.Success,
	}).Info("Compaction process completed")

	return result, nil
}

// findCompactionTasks finds partitions that need compaction
func (c *Compactor) findCompactionTasks(ctx context.Context) ([]CompactionTask, error) {
	tasks := make([]CompactionTask, 0)

	// Discover all databases from storage
	databases, err := c.findDatabases(ctx)
	if err != nil {
		c.logger.WithError(err).Error("Failed to discover databases")
		return nil, err
	}

	if len(databases) == 0 {
		c.logger.Info("No databases found for compaction")
		return tasks, nil
	}

	for _, database := range databases {
		// Find all measurements for this database
		measurements, err := c.findMeasurements(ctx, database)
		if err != nil {
			c.logger.WithError(err).WithField("database", database).Warn("Failed to find measurements")
			continue
		}

		for _, measurement := range measurements {
			// Find partitions for this measurement
			partitions, err := c.findPartitions(ctx, database, measurement)
			if err != nil {
				c.logger.WithError(err).WithFields(logrus.Fields{
					"database":    database,
					"measurement": measurement,
				}).Warn("Failed to find partitions")
				continue
			}

			for _, partition := range partitions {
				// Check if partition needs compaction
				task, shouldCompact := c.shouldCompactPartition(ctx, database, measurement, partition)
				if shouldCompact {
					tasks = append(tasks, task)
				}
			}
		}
	}

	return tasks, nil
}

// findDatabases discovers all databases from storage
func (c *Compactor) findDatabases(ctx context.Context) ([]string, error) {
	// List all objects at the root level to discover databases
	objects, err := c.storage.List(ctx, "")
	if err != nil {
		return nil, err
	}

	// Extract unique database names from object paths
	databaseSet := make(map[string]bool)
	for _, obj := range objects {
		if c.pathBuilder.IsSystemPath(obj.Key) {
			continue
		}

		database, _, _, _, _, _, err := c.pathBuilder.ParsePath(obj.Key)
		if err != nil {
			continue
		}

		if database != "" {
			databaseSet[database] = true
		}
	}

	// Convert to slice
	result := make([]string, 0, len(databaseSet))
	for database := range databaseSet {
		result = append(result, database)
	}

	// If no databases found, default to "default" database
	if len(result) == 0 {
		result = append(result, "default")
	}

	return result, nil
}

// findMeasurements finds all measurements for a database
func (c *Compactor) findMeasurements(ctx context.Context, database string) ([]string, error) {
	// Query storage backend to discover all measurements in the database
	prefix := c.pathBuilder.BuildDatabasePrefix(database)
	objects, err := c.storage.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Extract unique measurement names from object paths
	measurements := make(map[string]bool)
	for _, obj := range objects {
		if c.pathBuilder.IsSystemPath(obj.Key) {
			continue
		}

		_, measurement, _, _, _, _, err := c.pathBuilder.ParsePath(obj.Key)
		if err != nil {
			continue
		}

		measurements[measurement] = true
	}

	result := make([]string, 0, len(measurements))
	for measurement := range measurements {
		result = append(result, measurement)
	}

	return result, nil
}

// findPartitions finds all partitions for a measurement
func (c *Compactor) findPartitions(ctx context.Context, database, measurement string) ([]string, error) {
	prefix := c.pathBuilder.BuildMeasurementPrefix(database, measurement)
	objects, err := c.storage.List(ctx, prefix)
	if err != nil {
		return nil, err
	}

	partitions := make(map[string]bool)
	for _, obj := range objects {
		if c.pathBuilder.IsSystemPath(obj.Key) {
			continue
		}

		// Extract hour partition from path
		parsedDB, parsedMeasurement, year, month, day, hour, err := c.pathBuilder.ParsePath(obj.Key)
		if err != nil {
			continue
		}

		if parsedDB == database && parsedMeasurement == measurement {
			partition := c.pathBuilder.BuildHourlyPath(database, measurement, year, month, day, hour)
			partitions[partition] = true
		}
	}

	result := make([]string, 0, len(partitions))
	for partition := range partitions {
		result = append(result, partition)
	}

	return result, nil
}

// shouldCompactPartition determines if a partition should be compacted
func (c *Compactor) shouldCompactPartition(ctx context.Context, database, measurement, partition string) (CompactionTask, bool) {
	// List files in partition
	objects, err := c.storage.List(ctx, partition)
	if err != nil {
		c.logger.WithError(err).WithField("partition", partition).Warn("Failed to list partition files")
		return CompactionTask{}, false
	}

	// Filter Parquet files only
	parquetFiles := make([]storage.ObjectInfo, 0)
	for _, obj := range objects {
		if strings.HasSuffix(obj.Key, ".parquet") {
			parquetFiles = append(parquetFiles, obj)
		}
	}

	// Check minimum file count
	if len(parquetFiles) < c.config.MinFiles {
		return CompactionTask{}, false
	}

	// Check partition age
	partitionTime, err := c.pathBuilder.ParseTimeFromPath(partition)
	if err != nil {
		c.logger.WithError(err).WithField("partition", partition).Warn("Failed to parse partition time")
		return CompactionTask{}, false
	}

	minAge := time.Duration(c.config.MinAgeHours) * time.Hour
	if time.Since(partitionTime) < minAge {
		return CompactionTask{}, false
	}

	// Check if partition is locked
	lockKey := fmt.Sprintf("%s/%s", database, measurement)
	if c.isLocked(lockKey) {
		return CompactionTask{}, false
	}

	// Calculate priority based on file count and total size
	totalSize := int64(0)
	for _, obj := range parquetFiles {
		totalSize += obj.Size
	}

	priority := len(parquetFiles)
	if totalSize > 100*1024*1024 { // > 100MB
		priority += 10
	}

	task := CompactionTask{
		Database:    database,
		Measurement: measurement,
		Partition:   partition,
		Files:       parquetFiles,
		Priority:    priority,
		CreatedAt:   time.Now(),
	}

	return task, true
}

// compactPartition compacts a single partition
func (c *Compactor) compactPartition(ctx context.Context, task CompactionTask, result *CompactionResult) error {
	lockKey := fmt.Sprintf("%s/%s", task.Database, task.Measurement)

	// Acquire lock
	if !c.acquireLock(lockKey) {
		return fmt.Errorf("failed to acquire lock for partition %s", task.Partition)
	}
	defer c.releaseLock(lockKey)

	c.logger.WithFields(logrus.Fields{
		"database":    task.Database,
		"measurement": task.Measurement,
		"partition":   task.Partition,
		"files":       len(task.Files),
	}).Info("Compacting partition")

	start := time.Now()

	// Calculate total size
	totalSize := int64(0)
	for _, obj := range task.Files {
		totalSize += obj.Size
	}

	// Step 1: Read all Parquet files and collect records
	records := make([]arrow.Record, 0)
	for _, file := range task.Files {
		record, err := c.readParquetFile(ctx, file.Key)
		if err != nil {
			c.logger.WithError(err).WithField("file", file.Key).Error("Failed to read Parquet file")
			continue
		}
		if record != nil {
			records = append(records, record)
		}
	}

	if len(records) == 0 {
		return fmt.Errorf("no valid records found in partition")
	}

	defer func() {
		for _, record := range records {
			record.Release()
		}
	}()

	// Step 2: Merge records
	mergedRecord, err := c.mergeRecords(records)
	if err != nil {
		return fmt.Errorf("failed to merge records: %w", err)
	}
	defer mergedRecord.Release()

	// Step 3: Write optimized Parquet files
	targetSize := int64(c.config.TargetFileSizeMB) * 1024 * 1024
	newFiles, bytesWritten, err := c.writeOptimizedFiles(ctx, task, mergedRecord, targetSize)
	if err != nil {
		return fmt.Errorf("failed to write optimized files: %w", err)
	}

	// Step 4: Delete old files
	if err := c.deleteOldFiles(ctx, task.Files); err != nil {
		c.logger.WithError(err).Warn("Failed to delete old files (new files created successfully)")
	}

	// Update result
	result.FilesCompacted += int64(len(task.Files))
	result.FilesCreated += int64(len(newFiles))
	result.BytesProcessed += totalSize
	result.BytesWritten += bytesWritten
	result.PartitionsCompacted = append(result.PartitionsCompacted, task.Partition)

	c.logger.WithFields(logrus.Fields{
		"database":    task.Database,
		"measurement": task.Measurement,
		"partition":   task.Partition,
		"files_in":    len(task.Files),
		"files_out":   len(newFiles),
		"bytes_in":    totalSize,
		"bytes_out":   bytesWritten,
		"duration":    time.Since(start),
	}).Info("Partition compacted successfully")

	return nil
}

// readParquetFile reads a Parquet file from storage
func (c *Compactor) readParquetFile(ctx context.Context, key string) (arrow.Record, error) {
	// Download file from storage
	reader, err := c.storage.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get file from storage: %w", err)
	}
	defer reader.Close()

	// Read all data into memory
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read file data: %w", err)
	}

	// Create Parquet reader
	bytesReader := bytes.NewReader(data)
	pqFile, err := file.NewParquetReader(bytesReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer pqFile.Close()

	// Create Arrow file reader
	pqReader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{}, c.pool)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow reader: %w", err)
	}

	// Read all data as a table and convert to record
	table, err := pqReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	// Convert table to a single record
	if table.NumRows() == 0 {
		return nil, nil
	}

	// Get schema and columns
	schema := table.Schema()
	numCols := int(table.NumCols())
	cols := make([]arrow.Array, numCols)

	for i := 0; i < numCols; i++ {
		col := table.Column(i)
		chunks := col.Data().Chunks()

		if len(chunks) == 1 {
			cols[i] = chunks[0]
			chunks[0].Retain()
		} else {
			// Concatenate chunks
			concatenated, err := array.Concatenate(chunks, c.pool)
			if err != nil {
				return nil, fmt.Errorf("failed to concatenate chunks: %w", err)
			}
			cols[i] = concatenated
		}
	}

	record := array.NewRecord(schema, cols, table.NumRows())

	// Release columns
	for _, col := range cols {
		col.Release()
	}

	return record, nil
}

// mergeRecords merges multiple Arrow records into one
func (c *Compactor) mergeRecords(records []arrow.Record) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to merge")
	}

	if len(records) == 1 {
		records[0].Retain()
		return records[0], nil
	}

	// All records should have the same schema
	schema := records[0].Schema()
	numCols := int(records[0].NumCols())

	// Merge each column
	mergedCols := make([]arrow.Array, numCols)
	for colIdx := 0; colIdx < numCols; colIdx++ {
		arrays := make([]arrow.Array, len(records))
		for recIdx, record := range records {
			arrays[recIdx] = record.Column(colIdx)
		}

		concatenated, err := array.Concatenate(arrays, c.pool)
		if err != nil {
			// Release already created columns
			for i := 0; i < colIdx; i++ {
				mergedCols[i].Release()
			}
			return nil, fmt.Errorf("failed to concatenate column %d: %w", colIdx, err)
		}
		mergedCols[colIdx] = concatenated
	}

	// Calculate total rows
	totalRows := int64(0)
	for _, record := range records {
		totalRows += record.NumRows()
	}

	merged := array.NewRecord(schema, mergedCols, totalRows)

	// Release columns
	for _, col := range mergedCols {
		col.Release()
	}

	return merged, nil
}

// writeOptimizedFiles writes merged records as optimized Parquet files
func (c *Compactor) writeOptimizedFiles(ctx context.Context, task CompactionTask, record arrow.Record, targetSize int64) ([]string, int64, error) {
	// For simplicity, write as a single file
	// In production, you might split large records into multiple files

	filename := fmt.Sprintf("compacted_%d.parquet", time.Now().UnixNano())

	// Parse partition path to get year/month/day/hour
	_, _, year, month, day, hour, err := c.pathBuilder.ParsePath(task.Partition)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse partition path: %w", err)
	}

	basePath := c.pathBuilder.BuildHourlyPath(task.Database, task.Measurement, year, month, day, hour)
	key := fmt.Sprintf("%s/%s", basePath, filename)

	// Write to buffer
	buf := new(bytes.Buffer)

	// Create Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	// Create Parquet file writer
	pqWriter, err := pqarrow.NewFileWriter(record.Schema(), buf, writerProps, arrowProps)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create Parquet writer: %w", err)
	}

	// Write record
	if err := pqWriter.Write(record); err != nil {
		pqWriter.Close()
		return nil, 0, fmt.Errorf("failed to write record: %w", err)
	}

	if err := pqWriter.Close(); err != nil {
		return nil, 0, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	// Upload to storage
	data := buf.Bytes()
	if err := c.storage.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		return nil, 0, fmt.Errorf("failed to upload file: %w", err)
	}

	return []string{key}, int64(len(data)), nil
}

// deleteOldFiles deletes the old Parquet files after compaction
func (c *Compactor) deleteOldFiles(ctx context.Context, files []storage.ObjectInfo) error {
	keys := make([]string, len(files))
	for i, file := range files {
		keys[i] = file.Key
	}

	if len(keys) == 0 {
		return nil
	}

	// Use batch delete if available
	if err := c.storage.DeleteBatch(ctx, keys); err != nil {
		// Fall back to individual deletes
		for _, key := range keys {
			if err := c.storage.Delete(ctx, key); err != nil {
				c.logger.WithError(err).WithField("key", key).Warn("Failed to delete old file")
			}
		}
	}

	c.logger.WithField("count", len(keys)).Debug("Deleted old files")
	return nil
}

// isLocked checks if a partition is locked
func (c *Compactor) isLocked(lockKey string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	lock, exists := c.locks[lockKey]
	if !exists {
		return false
	}

	// Check if lock has expired
	if time.Now().After(lock.ExpiresAt) {
		delete(c.locks, lockKey)
		return false
	}

	return true
}

// acquireLock acquires a lock for a partition
func (c *Compactor) acquireLock(lockKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already locked
	if _, exists := c.locks[lockKey]; exists {
		return false
	}

	// Create lock
	lock := &CompactionLock{
		Partition: lockKey,
		WorkerID:  fmt.Sprintf("worker-%d", time.Now().UnixNano()),
		StartTime: time.Now(),
		ExpiresAt: time.Now().Add(time.Duration(c.config.LockTTLHours) * time.Hour),
	}

	c.locks[lockKey] = lock
	return true
}

// releaseLock releases a lock for a partition
func (c *Compactor) releaseLock(lockKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.locks, lockKey)
}

// GetLocks returns current active locks
func (c *Compactor) GetLocks() map[string]*CompactionLock {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Copy the locks map
	locks := make(map[string]*CompactionLock)
	for k, v := range c.locks {
		// Remove expired locks
		if time.Now().Before(v.ExpiresAt) {
			locks[k] = v
		}
	}

	// Update internal locks map
	c.locks = locks
	return locks
}

// CleanupExpiredLocks removes expired locks
func (c *Compactor) CleanupExpiredLocks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for key, lock := range c.locks {
		if now.After(lock.ExpiresAt) {
			delete(c.locks, key)
			c.logger.WithFields(logrus.Fields{
				"partition": lock.Partition,
				"worker_id": lock.WorkerID,
			}).Debug("Cleaned up expired lock")
		}
	}
}
