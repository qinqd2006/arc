package v1

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/ingestion"
	storagelib "github.com/arc-core/arc-go/internal/storage"
	"github.com/arc-core/arc-go/internal/wal"
	"github.com/arc-core/arc-go/pkg/lineprotocol"
	"github.com/arc-core/arc-go/pkg/msgpack"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// IngestionHandler handles data ingestion endpoints
type IngestionHandler struct {
	config         *config.Config
	storage        storagelib.Backend
	buffer         *ingestion.Buffer
	parquetWriter  *ingestion.ParquetWriter
	pathBuilder    storagelib.PathBuilder
	msgpackDecoder *msgpack.Decoder
	lineParser     *lineprotocol.Parser
	wal            *wal.WAL
	logger         *logrus.Logger
}

// NewIngestionHandler creates a new ingestion handler
func NewIngestionHandler(
	cfg *config.Config,
	storage storagelib.Backend,
	logger *logrus.Logger,
) *IngestionHandler {
	if logger == nil {
		logger = logrus.New()
	}

	// Create components
	pool := memory.NewGoAllocator()
	pathBuilder := storagelib.NewDefaultPathBuilder()
	msgpackDecoder := msgpack.NewDecoder(pool, logger)
	lineParser := lineprotocol.NewParser(pool, logger)

	// Create Parquet writer
	parquetConfig := ingestion.ParquetWriterConfig{
		Compression:  cfg.Ingestion.Compression,
		RowGroupSize: 10000,
		BatchSize:    1000,
	}
	parquetWriter := ingestion.NewParquetWriter(parquetConfig, pool, storage, pathBuilder, logger)

	// Initialize WAL if enabled
	var walInstance *wal.WAL
	if cfg.WAL.Enabled {
		var err error
		walInstance, err = wal.NewWAL(&cfg.WAL, storage, pathBuilder, logger)
		if err != nil {
			logger.WithError(err).Warn("Failed to initialize WAL, continuing without WAL")
			walInstance = nil
		} else {
			logger.Info("WAL initialized successfully for ingestion")
		}
	}

	handler := &IngestionHandler{
		config:         cfg,
		storage:        storage,
		parquetWriter:  parquetWriter,
		pathBuilder:    pathBuilder,
		msgpackDecoder: msgpackDecoder,
		lineParser:     lineParser,
		wal:            walInstance,
		logger:         logger,
	}

	// Create buffer with handler as flush handler
	bufferConfig := ingestion.BufferConfig{
		BufferSize:       cfg.Ingestion.BufferSize,
		BufferAgeSeconds: time.Duration(cfg.Ingestion.BufferAgeSeconds) * time.Second,
		MaxPendingOps:    cfg.GoRuntime.MaxPendingOps,
	}
	buffer := ingestion.NewBuffer(bufferConfig, pool, logger, handler)
	handler.buffer = buffer

	// Start background flush processor
	go handler.flushProcessor()

	return handler
}

// MessagePack handles MessagePack data ingestion
func (h *IngestionHandler) MessagePack(c *gin.Context) {
	start := time.Now()

	// Read request body
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(c.Request.Body); err != nil {
		h.logger.WithError(err).Error("Failed to read request body")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to read request body",
		})
		return
	}

	data := buf.Bytes()
	if len(data) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Empty request body",
		})
		return
	}

	// Check for gzip compression
	contentEncoding := c.GetHeader("Content-Encoding")
	if strings.ToLower(contentEncoding) == "gzip" {
		// Decompress gzip data
		gzipReader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			h.logger.WithError(err).Error("Failed to create gzip reader")
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to decompress gzip data",
			})
			return
		}
		defer gzipReader.Close()

		// Read decompressed data
		decompressed, err := io.ReadAll(gzipReader)
		if err != nil {
			h.logger.WithError(err).Error("Failed to read decompressed data")
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to read decompressed data",
			})
			return
		}

		data = decompressed
		h.logger.WithFields(logrus.Fields{
			"compressed_size":   len(buf.Bytes()),
			"decompressed_size": len(data),
			"compression_ratio": float64(len(buf.Bytes())) / float64(len(data)),
		}).Debug("MessagePack: Decompressed gzip data")
	}

	// Get database from query parameter or default
	database := c.DefaultQuery("db", h.config.GetDatabase())

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"data_size":   len(data),
		"remote_addr": c.ClientIP(),
	}).Debug("MessagePack: Processing request")

	// Decode MessagePack to Arrow record
	record, err := h.msgpackDecoder.DecodeToArrow(data)
	if err != nil {
		h.logger.WithError(err).WithField("data_size", len(data)).Error("MessagePack decode failed")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Failed to decode MessagePack data",
			"details": err.Error(),
		})
		return
	}
	defer record.Release()

	// Get measurement from schema metadata
	measurement := "default"
	if meta := record.Schema().Metadata(); meta.Len() > 0 {
		if idx := meta.FindKey("measurement"); idx >= 0 {
			measurement = meta.Values()[idx]
		}
	}

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"measurement": measurement,
		"rows":        record.NumRows(),
	}).Debug("MessagePack: Extracted measurement from metadata")

	// Write to WAL first (if enabled) for durability
	if h.wal != nil {
		if err := h.writeToWAL(c.Request.Context(), database, measurement, data, nil); err != nil {
			h.logger.WithError(err).Warn("Failed to write to WAL, continuing to buffer")
		}
	}

	// Add to buffer
	if err := h.buffer.Add(c.Request.Context(), record); err != nil {
		h.logger.WithError(err).Error("Failed to add record to buffer")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to buffer data",
		})
		return
	}

	// Return success response (204 No Content for high-performance clients)
	c.Status(http.StatusNoContent)

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"measurement": measurement,
		"rows":        record.NumRows(),
		"took":        time.Since(start),
	}).Info("MessagePack: Request processed successfully")
}

// LineProtocol handles InfluxDB Line Protocol ingestion
func (h *IngestionHandler) LineProtocol(c *gin.Context) {
	start := time.Now()

	// Read request body
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(c.Request.Body); err != nil {
		h.logger.WithError(err).Error("Failed to read request body")
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Failed to read request body",
		})
		return
	}

	data := buf.Bytes()
	if len(data) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Empty request body",
		})
		return
	}

	// Check for gzip compression
	contentEncoding := c.GetHeader("Content-Encoding")
	if strings.ToLower(contentEncoding) == "gzip" {
		// Decompress gzip data
		gzipReader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			h.logger.WithError(err).Error("Failed to create gzip reader")
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to decompress gzip data",
			})
			return
		}
		defer gzipReader.Close()

		// Read decompressed data
		decompressed, err := io.ReadAll(gzipReader)
		if err != nil {
			h.logger.WithError(err).Error("Failed to read decompressed data")
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Failed to read decompressed data",
			})
			return
		}

		data = decompressed
		h.logger.WithFields(logrus.Fields{
			"compressed_size":   len(buf.Bytes()),
			"decompressed_size": len(data),
			"compression_ratio": float64(len(buf.Bytes())) / float64(len(data)),
		}).Debug("LineProtocol: Decompressed gzip data")
	}

	// Get database from query parameter or default
	database := c.DefaultQuery("db", h.config.GetDatabase())

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"data_size":   len(data),
		"remote_addr": c.ClientIP(),
	}).Debug("LineProtocol: Processing request")

	// Parse Line Protocol to points
	points, err := h.lineParser.ParseLines(string(data))
	if err != nil {
		h.logger.WithError(err).WithField("data_size", len(data)).Error("LineProtocol parse failed")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Failed to parse Line Protocol data",
			"details": err.Error(),
		})
		return
	}

	if len(points) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "No valid points found in request",
		})
		return
	}

	// Convert points to Arrow record
	record, err := h.lineParser.ToArrow(points)
	if err != nil {
		h.logger.WithError(err).Error("Failed to convert points to Arrow")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Failed to convert data",
		})
		return
	}
	defer record.Release()

	// Write to WAL first (if enabled) for durability
	if h.wal != nil {
		// Write raw line protocol data to WAL with database context
		headers := map[string]string{
			"content-type": "application/x-influxdb-lineprotocol",
		}
		if err := h.writeToWAL(c.Request.Context(), database, "line_protocol", data, headers); err != nil {
			h.logger.WithError(err).Warn("Failed to write to WAL, continuing to buffer")
		}
	}

	// Group by measurement
	measurements := make(map[string][]arrow.Record)
	h.groupRecordsByMeasurement(record, measurements)

	// Add each measurement's records to buffer
	totalRows := int64(0)
	for measurement, records := range measurements {
		for _, rec := range records {
			if err := h.buffer.Add(c.Request.Context(), rec); err != nil {
				h.logger.WithError(err).WithField("measurement", measurement).Error("Failed to add record to buffer")
				continue
			}
			totalRows += rec.NumRows()
		}
	}

	// Return success response (204 No Content for high-performance clients)
	c.Status(http.StatusNoContent)

	h.logger.WithFields(logrus.Fields{
		"database":     database,
		"points":       len(points),
		"rows":         totalRows,
		"measurements": len(measurements),
		"took":         time.Since(start),
	}).Info("LineProtocol: Request processed successfully")
}

// InfluxDB2 handles InfluxDB 2.x API compatibility
func (h *IngestionHandler) InfluxDB2(c *gin.Context) {
	// InfluxDB 2.x uses the same Line Protocol format but different API structure
	// Extract parameters from request
	org := c.Param("org")
	bucket := c.Param("bucket")

	// Map org+bucket to our database concept
	database := bucket
	if org != "" {
		database = org + "_" + bucket
	}

	// Set database query parameter for LineProtocol handler
	c.Request.URL.RawQuery = "db=" + database

	// Delegate to LineProtocol handler
	h.LineProtocol(c)
}

// flushProcessor processes buffered records and writes them to Parquet
func (h *IngestionHandler) flushProcessor() {
	ticker := time.NewTicker(time.Duration(h.config.Ingestion.BufferAgeSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.flushBuffer()
		}
	}
}

// flushBuffer flushes the current buffer contents
func (h *IngestionHandler) flushBuffer() {
	ctx := context.Background()

	// Get current buffer size
	if h.buffer.Size() == 0 {
		return
	}

	start := time.Now()
	h.logger.Debug("Ingestion: Starting buffer flush")

	// In a real implementation, this would:
	// 1. Get records from buffer
	// 2. Group by database/measurement
	// 3. Write to Parquet via parquetWriter
	// 4. Handle errors and retry logic

	// For now, just trigger the buffer flush
	if err := h.buffer.Flush(ctx); err != nil {
		h.logger.WithError(err).Error("Buffer flush failed")
	}

	h.logger.WithField("took", time.Since(start)).Debug("Ingestion: Buffer flush completed")
}

// groupRecordsByMeasurement groups records by measurement
func (h *IngestionHandler) groupRecordsByMeasurement(record arrow.Record, result map[string][]arrow.Record) {
	// Find measurement column index
	measurementColIdx := -1
	for i := 0; i < int(record.NumCols()); i++ {
		if record.Schema().Field(i).Name == "measurement" {
			measurementColIdx = i
			break
		}
	}

	if measurementColIdx == -1 {
		// Fallback to "default" if measurement column not found
		h.logger.Warn("No measurement column found in record, using 'default'")
		measurement := "default"
		if _, exists := result[measurement]; !exists {
			result[measurement] = make([]arrow.Record, 0)
		}
		record.Retain()
		result[measurement] = append(result[measurement], record)
		return
	}

	// Get measurement column
	measurementCol := record.Column(measurementColIdx)
	strArray, ok := measurementCol.(*array.String)
	if !ok {
		h.logger.Warn("Measurement column is not a string array, using 'default'")
		measurement := "default"
		if _, exists := result[measurement]; !exists {
			result[measurement] = make([]arrow.Record, 0)
		}
		record.Retain()
		result[measurement] = append(result[measurement], record)
		return
	}

	// All rows should have the same measurement (enforced by parser)
	if strArray.Len() > 0 {
		measurement := strArray.Value(0)
		if _, exists := result[measurement]; !exists {
			result[measurement] = make([]arrow.Record, 0)
		}
		record.Retain()
		result[measurement] = append(result[measurement], record)

		h.logger.WithFields(logrus.Fields{
			"measurement": measurement,
			"rows":        record.NumRows(),
		}).Debug("Grouped record by measurement")
	}
}

// extractMeasurementFromRecord extracts the measurement name from an Arrow record
func (h *IngestionHandler) extractMeasurementFromRecord(record arrow.Record) string {
	// Get measurement from schema metadata
	if meta := record.Schema().Metadata(); meta.Len() > 0 {
		if idx := meta.FindKey("measurement"); idx >= 0 {
			measurement := meta.Values()[idx]
			if measurement != "" {
				return measurement
			}
		}
	}

	// Fallback: check for measurement column
	for i := 0; i < int(record.NumCols()); i++ {
		if record.Schema().Field(i).Name == "measurement" {
			col := record.Column(i)
			if strArray, ok := col.(*array.String); ok && strArray.Len() > 0 {
				measurement := strArray.Value(0)
				if measurement != "" {
					return measurement
				}
			}
		}
	}

	// Fallback to "default" if measurement not found
	h.logger.Warn("Could not extract measurement from record, using 'default'")
	return "default"
}

// GetStats returns ingestion statistics
func (h *IngestionHandler) GetStats(c *gin.Context) {
	bufferMetrics := h.buffer.GetMetrics()
	writerMetrics := h.parquetWriter.GetMetrics()

	stats := gin.H{
		"buffer": bufferMetrics,
		"writer": writerMetrics,
	}

	// Add WAL stats if enabled
	if h.wal != nil {
		stats["wal"] = h.wal.GetMetrics()
		stats["wal_enabled"] = true
	} else {
		stats["wal_enabled"] = false
	}

	c.JSON(http.StatusOK, stats)
}

// writeToWAL writes data to the Write-Ahead Log for durability
func (h *IngestionHandler) writeToWAL(ctx context.Context, database, measurement string, data []byte, headers map[string]string) error {
	if h.wal == nil {
		return nil
	}

	return h.wal.Write(ctx, database, measurement, data, headers)
}

// Close gracefully shuts down the ingestion handler
func (h *IngestionHandler) Close(ctx context.Context) error {
	h.logger.Info("IngestionHandler: Shutting down")

	// Flush remaining data
	h.flushBuffer()

	// Close components
	if err := h.buffer.Close(ctx); err != nil {
		h.logger.WithError(err).Error("Failed to close buffer")
	}

	if err := h.parquetWriter.Close(); err != nil {
		h.logger.WithError(err).Error("Failed to close Parquet writer")
	}

	// Close WAL if enabled
	if h.wal != nil {
		if err := h.wal.Close(); err != nil {
			h.logger.WithError(err).Error("Failed to close WAL")
		}
	}

	h.logger.Info("IngestionHandler: Shutdown complete")
	return nil
}

// Flush implements the FlushHandler interface - writes buffered records to Parquet storage
func (h *IngestionHandler) Flush(ctx context.Context, records []arrow.Record) error {
	if len(records) == 0 {
		return nil
	}

	start := time.Now()
	h.logger.WithField("record_count", len(records)).Debug("IngestionHandler: Flushing records to storage")

	// Group records by measurement
	// In a production system, you'd extract database/measurement from record metadata
	// For now, we'll use a default database and extract measurement from schema
	database := h.config.GetDatabase()

	recordsWritten := 0
	var lastErr error

	// Write each record to Parquet storage
	for _, record := range records {
		// Extract measurement name from the "measurement" column in the record
		measurement := h.extractMeasurementFromRecord(record)

		// Write record to Parquet
		if err := h.parquetWriter.WriteRecord(ctx, record, database, measurement); err != nil {
			h.logger.WithError(err).WithFields(logrus.Fields{
				"database":    database,
				"measurement": measurement,
				"rows":        record.NumRows(),
			}).Error("Failed to write record to Parquet")
			lastErr = err
			continue
		}

		recordsWritten++
	}

	h.logger.WithFields(logrus.Fields{
		"records_written": recordsWritten,
		"total_records":   len(records),
		"database":        database,
		"took":            time.Since(start),
	}).Info("IngestionHandler: Flush completed")

	return lastErr
}
