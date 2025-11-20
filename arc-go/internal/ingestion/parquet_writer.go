package ingestion

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/sirupsen/logrus"
)

// ParquetWriterConfig holds configuration for Parquet writing
type ParquetWriterConfig struct {
	Compression  string `toml:"compression"`
	RowGroupSize int    `toml:"row_group_size"`
	BatchSize    int    `toml:"batch_size"`
}

// ParquetWriter handles writing Arrow records to Parquet format
type ParquetWriter struct {
	config      ParquetWriterConfig
	pool        memory.Allocator
	storage     storage.Backend
	pathBuilder storage.PathBuilder
	logger      *logrus.Logger

	// Metrics
	metrics ParquetWriterMetrics
}

// ParquetWriterMetrics holds Parquet writer performance metrics
type ParquetWriterMetrics struct {
	FilesWritten     int64         `json:"files_written"`
	RecordsWritten   int64         `json:"records_written"`
	BytesWritten     int64         `json:"bytes_written"`
	WriteOperations  int64         `json:"write_operations"`
	ErrorCount       int64         `json:"error_count"`
	LastWrite        time.Time     `json:"last_write"`
	AverageWriteTime time.Duration `json:"average_write_time"`
	CompressionRatio float64       `json:"compression_ratio"`
}

// NewParquetWriter creates a new Parquet writer
func NewParquetWriter(
	config ParquetWriterConfig,
	pool memory.Allocator,
	storage storage.Backend,
	pathBuilder storage.PathBuilder,
	logger *logrus.Logger,
) *ParquetWriter {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	if logger == nil {
		logger = logrus.New()
	}

	// Set defaults
	if config.Compression == "" {
		config.Compression = "snappy"
	}
	if config.RowGroupSize <= 0 {
		config.RowGroupSize = 10000
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}

	return &ParquetWriter{
		config:      config,
		pool:        pool,
		storage:     storage,
		pathBuilder: pathBuilder,
		logger:      logger,
		metrics:     ParquetWriterMetrics{},
	}
}

// WriteRecord writes a single Arrow record to Parquet storage
func (w *ParquetWriter) WriteRecord(ctx context.Context, record arrow.Record, database, measurement string) error {
	if record.NumRows() == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		w.metrics.WriteOperations++
		w.metrics.LastWrite = start
		w.metrics.RecordsWritten += record.NumRows()

		// Update average write time
		if w.metrics.WriteOperations > 0 {
			w.metrics.AverageWriteTime = time.Duration(
				int64(w.metrics.AverageWriteTime)*w.metrics.WriteOperations/int64(w.metrics.WriteOperations+1) +
					int64(time.Since(start))/int64(w.metrics.WriteOperations+1),
			)
		}
	}()

	// Generate filename and path
	timestamp := time.Now()
	filename := w.pathBuilder.BuildParquetFileName(measurement, timestamp)
	directory := w.pathBuilder.BuildMeasurementPath(database, measurement, timestamp)
	key := fmt.Sprintf("%s/%s", directory, filename)

	w.logger.WithFields(logrus.Fields{
		"measurement": measurement,
		"database":    database,
		"rows":        record.NumRows(),
		"columns":     record.NumCols(),
		"key":         key,
	}).Debug("ParquetWriter: Writing record")

	// Convert Arrow record to Parquet
	parquetData, size, err := w.arrowToParquet(record)
	if err != nil {
		w.metrics.ErrorCount++
		return fmt.Errorf("failed to convert Arrow to Parquet: %w", err)
	}

	// Write to storage
	if err := w.storage.Put(ctx, key, bytes.NewReader(parquetData), int64(size)); err != nil {
		w.metrics.ErrorCount++
		return fmt.Errorf("failed to write Parquet to storage: %w", err)
	}

	w.metrics.FilesWritten++
	w.metrics.BytesWritten += int64(size)

	w.logger.WithFields(logrus.Fields{
		"measurement": measurement,
		"rows":        record.NumRows(),
		"size_bytes":  size,
		"took":        time.Since(start),
		"key":         key,
	}).Info("ParquetWriter: Record written successfully")

	return nil
}

// WriteBatch writes multiple Arrow records to a single Parquet file
func (w *ParquetWriter) WriteBatch(ctx context.Context, records []arrow.Record, database, measurement string) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		w.metrics.WriteOperations++
		w.metrics.LastWrite = start

		totalRows := int64(0)
		for _, record := range records {
			totalRows += record.NumRows()
		}
		w.metrics.RecordsWritten += totalRows

		// Update average write time
		if w.metrics.WriteOperations > 0 {
			w.metrics.AverageWriteTime = time.Duration(
				int64(w.metrics.AverageWriteTime)*w.metrics.WriteOperations/int64(w.metrics.WriteOperations+1) +
					int64(time.Since(start))/int64(w.metrics.WriteOperations+1),
			)
		}
	}()

	// Merge records if needed
	mergedRecord, err := w.mergeRecords(records)
	if err != nil {
		w.metrics.ErrorCount++
		return fmt.Errorf("failed to merge records: %w", err)
	}
	defer mergedRecord.Release()

	// Generate filename and path
	timestamp := time.Now()
	filename := w.pathBuilder.BuildParquetFileName(measurement, timestamp)
	directory := w.pathBuilder.BuildMeasurementPath(database, measurement, timestamp)
	key := fmt.Sprintf("%s/%s", directory, filename)

	totalRows := int64(0)
	for _, record := range records {
		totalRows += record.NumRows()
	}

	w.logger.WithFields(logrus.Fields{
		"measurement":    measurement,
		"database":       database,
		"record_count":   len(records),
		"total_rows":     totalRows,
		"merged_columns": mergedRecord.NumCols(),
		"key":            key,
	}).Debug("ParquetWriter: Writing batch")

	// Convert Arrow record to Parquet
	parquetData, size, err := w.arrowToParquet(mergedRecord)
	if err != nil {
		w.metrics.ErrorCount++
		return fmt.Errorf("failed to convert Arrow to Parquet: %w", err)
	}

	// Write to storage
	if err := w.storage.Put(ctx, key, bytes.NewReader(parquetData), int64(size)); err != nil {
		w.metrics.ErrorCount++
		return fmt.Errorf("failed to write Parquet to storage: %w", err)
	}

	w.metrics.FilesWritten++
	w.metrics.BytesWritten += int64(size)

	// Update compression ratio
	if w.metrics.RecordsWritten > 0 {
		// Estimate uncompressed size (rows * columns * 8 bytes average)
		estimatedUncompressed := totalRows * int64(mergedRecord.NumCols()) * 8
		if estimatedUncompressed > 0 {
			w.metrics.CompressionRatio = float64(size) / float64(estimatedUncompressed)
		}
	}

	w.logger.WithFields(logrus.Fields{
		"measurement":  measurement,
		"record_count": len(records),
		"total_rows":   totalRows,
		"size_bytes":   size,
		"compression":  w.metrics.CompressionRatio,
		"took":         time.Since(start),
		"key":          key,
	}).Info("ParquetWriter: Batch written successfully")

	return nil
}

// arrowToParquet converts an Arrow record to Parquet bytes
func (w *ParquetWriter) arrowToParquet(record arrow.Record) ([]byte, int, error) {
	// Create in-memory buffer
	buf := new(bytes.Buffer)

	// Get compression codec
	compressionCodec := w.getCompressionCodec()

	// Create Parquet writer properties
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compressionCodec),
		parquet.WithMaxRowGroupLength(int64(w.config.RowGroupSize)),
	)

	// Create Arrow writer properties
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(w.pool))

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), buf, props, arrProps)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create Parquet writer: %w", err)
	}
	defer writer.Close()

	// Write record
	if err := writer.Write(record); err != nil {
		return nil, 0, fmt.Errorf("failed to write record to Parquet: %w", err)
	}

	// Close writer to finalize file
	if err := writer.Close(); err != nil {
		return nil, 0, fmt.Errorf("failed to close Parquet writer: %w", err)
	}

	// Return buffer contents
	data := buf.Bytes()
	return data, len(data), nil
}

// getCompressionCodec returns the Parquet compression codec based on config
func (w *ParquetWriter) getCompressionCodec() compress.Compression {
	switch strings.ToLower(w.config.Compression) {
	case "snappy":
		return compress.Codecs.Snappy
	case "gzip":
		return compress.Codecs.Gzip
	case "brotli":
		return compress.Codecs.Brotli
	case "zstd":
		return compress.Codecs.Zstd
	case "lz4":
		return compress.Codecs.Lz4
	case "none", "uncompressed":
		return compress.Codecs.Uncompressed
	default:
		return compress.Codecs.Snappy // Default to Snappy
	}
}

// mergeRecords merges multiple Arrow records into a single record
func (w *ParquetWriter) mergeRecords(records []arrow.Record) (arrow.Record, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to merge")
	}
	if len(records) == 1 {
		records[0].Retain()
		return records[0], nil
	}

	// All records should have the same schema
	schema := records[0].Schema()
	for i, record := range records {
		if !record.Schema().Equal(schema) {
			return nil, fmt.Errorf("record %d has different schema", i)
		}
	}

	// Build concatenated arrays for each column
	numCols := int(records[0].NumCols())
	mergedCols := make([]arrow.Array, numCols)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		// Collect all arrays for this column
		arrays := make([]arrow.Array, len(records))
		for recIdx, record := range records {
			arrays[recIdx] = record.Column(colIdx)
		}

		// Concatenate arrays
		concatenated, err := array.Concatenate(arrays, w.pool)
		if err != nil {
			return nil, fmt.Errorf("failed to concatenate column %d: %w", colIdx, err)
		}
		mergedCols[colIdx] = concatenated
	}

	// Create new record with concatenated columns
	merged := array.NewRecord(schema, mergedCols, -1)

	return merged, nil
}

// GetMetrics returns current writer metrics
func (w *ParquetWriter) GetMetrics() ParquetWriterMetrics {
	return w.metrics
}

// ResetMetrics resets the writer metrics
func (w *ParquetWriter) ResetMetrics() {
	w.metrics = ParquetWriterMetrics{}
}

// Close closes the Parquet writer
func (w *ParquetWriter) Close() error {
	w.logger.Info("ParquetWriter: Closed")
	return nil
}

// EstimateSize estimates the size of a record when written to Parquet
func (w *ParquetWriter) EstimateSize(record arrow.Record) int64 {
	if record.NumRows() == 0 {
		return 0
	}

	// Rough estimation: rows * columns * 8 bytes / compression ratio
	baseSize := record.NumRows() * int64(record.NumCols()) * 8

	// Apply compression ratio estimate
	compressionRatio := 0.3 // Default compression ratio
	if w.metrics.CompressionRatio > 0 {
		compressionRatio = w.metrics.CompressionRatio
	}

	return int64(float64(baseSize) * compressionRatio)
}

// ValidateRecord validates that a record can be written to Parquet
func (w *ParquetWriter) ValidateRecord(record arrow.Record) error {
	if record == nil {
		return fmt.Errorf("record is nil")
	}
	if record.NumRows() == 0 {
		return fmt.Errorf("record has no rows")
	}
	if record.NumCols() == 0 {
		return fmt.Errorf("record has no columns")
	}
	if record.Schema() == nil {
		return fmt.Errorf("record has no schema")
	}

	// Check for invalid column names
	for i := int64(0); i < record.NumCols(); i++ {
		field := record.Schema().Field(int(i))
		if strings.ContainsAny(field.Name, "/\\:*?\"<>|") {
			return fmt.Errorf("invalid column name: %s", field.Name)
		}
	}

	return nil
}
