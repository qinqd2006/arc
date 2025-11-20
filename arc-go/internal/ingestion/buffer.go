package ingestion

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/sirupsen/logrus"
)

// BufferConfig holds configuration for the in-memory buffer
type BufferConfig struct {
	BufferSize       int           `toml:"buffer_size"`
	BufferAgeSeconds time.Duration `toml:"buffer_age_seconds"`
	MaxPendingOps    int           `toml:"max_pending_ops"`
}

// Buffer manages in-memory buffering of Arrow records before flushing to storage
type Buffer struct {
	config       BufferConfig
	pool         memory.Allocator
	logger       *logrus.Logger
	mu           sync.RWMutex
	records      []arrow.Record
	flushCh      chan struct{}
	done         chan struct{}
	wg           sync.WaitGroup
	lastFlush    time.Time
	flushHandler FlushHandler

	// Metrics
	metrics BufferMetrics
}

// BufferMetrics holds buffer performance metrics
type BufferMetrics struct {
	RecordsReceived   int64     `json:"records_received"`
	RecordsFlushed    int64     `json:"records_flushed"`
	FlushOperations   int64     `json:"flush_operations"`
	ErrorCount        int64     `json:"error_count"`
	LastFlush         time.Time `json:"last_flush"`
	AverageFlushSize  int64     `json:"average_flush_size"`
	CurrentBufferSize int       `json:"current_buffer_size"`
	MaxBufferSize     int       `json:"max_buffer_size"`
}

// FlushHandler handles buffer flush operations
type FlushHandler interface {
	Flush(ctx context.Context, records []arrow.Record) error
}

// NewBuffer creates a new in-memory buffer
func NewBuffer(config BufferConfig, pool memory.Allocator, logger *logrus.Logger, flushHandler FlushHandler) *Buffer {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	if logger == nil {
		logger = logrus.New()
	}

	buffer := &Buffer{
		config:       config,
		pool:         pool,
		logger:       logger,
		records:      make([]arrow.Record, 0, config.BufferSize),
		flushCh:      make(chan struct{}, 1),
		done:         make(chan struct{}),
		lastFlush:    time.Now(),
		flushHandler: flushHandler,
		metrics: BufferMetrics{
			MaxBufferSize: config.BufferSize,
		},
	}

	// Start background flush goroutine
	buffer.wg.Add(1)
	go buffer.flushLoop()

	return buffer
}

// Add adds a record to the buffer
func (b *Buffer) Add(ctx context.Context, record arrow.Record) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Update metrics
	b.metrics.RecordsReceived += record.NumRows()
	b.metrics.CurrentBufferSize = len(b.records)

	// Add record to buffer
	b.records = append(b.records, record)
	record.Retain() // Retain record for buffer management

	// Check if buffer should flush
	shouldFlush := len(b.records) >= b.config.BufferSize

	b.logger.WithFields(logrus.Fields{
		"records":      record.NumRows(),
		"buffer_size":  len(b.records),
		"max_size":     b.config.BufferSize,
		"should_flush": shouldFlush,
	}).Debug("Buffer: Added record")

	if shouldFlush {
		b.triggerFlush()
	}

	return nil
}

// AddBatch adds multiple records to the buffer
func (b *Buffer) AddBatch(ctx context.Context, records []arrow.Record) error {
	if len(records) == 0 {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Update metrics
	totalRows := int64(0)
	for _, record := range records {
		totalRows += record.NumRows()
		record.Retain() // Retain record for buffer management
	}
	b.metrics.RecordsReceived += totalRows
	b.metrics.CurrentBufferSize = len(b.records) + len(records)

	// Add records to buffer
	b.records = append(b.records, records...)

	// Check if buffer should flush
	shouldFlush := len(b.records) >= b.config.BufferSize

	b.logger.WithFields(logrus.Fields{
		"records_count": len(records),
		"total_rows":    totalRows,
		"buffer_size":   len(b.records),
		"max_size":      b.config.BufferSize,
		"should_flush":  shouldFlush,
	}).Debug("Buffer: Added batch")

	if shouldFlush {
		b.triggerFlush()
	}

	return nil
}

// Flush triggers an immediate flush of the buffer
func (b *Buffer) Flush(ctx context.Context) error {
	b.triggerFlush()
	return nil
}

// triggerFlush sends a signal to flush the buffer
func (b *Buffer) triggerFlush() {
	select {
	case b.flushCh <- struct{}{}:
		b.logger.Debug("Buffer: Flush triggered")
	default:
		// Flush already pending
	}
}

// flushLoop runs the background flush loop
func (b *Buffer) flushLoop() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.config.BufferAgeSeconds)
	defer ticker.Stop()

	for {
		select {
		case <-b.flushCh:
			b.flush()
		case <-ticker.C:
			b.checkAutoFlush()
		case <-b.done:
			// Final flush before shutdown
			b.flush()
			return
		}
	}
}

// checkAutoFlush checks if buffer should be flushed based on age
func (b *Buffer) checkAutoFlush() {
	b.mu.RLock()
	timeSinceLastFlush := time.Since(b.lastFlush)
	bufferSize := len(b.records)
	b.mu.RUnlock()

	if bufferSize > 0 && timeSinceLastFlush >= b.config.BufferAgeSeconds {
		b.logger.WithFields(logrus.Fields{
			"buffer_size":           bufferSize,
			"time_since_last_flush": timeSinceLastFlush,
			"max_age":               b.config.BufferAgeSeconds,
		}).Debug("Buffer: Auto-flush triggered by age")
		b.triggerFlush()
	}
}

// flush performs the actual buffer flush
func (b *Buffer) flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return
	}

	start := time.Now()

	// Extract records to flush
	recordsToFlush := make([]arrow.Record, len(b.records))
	copy(recordsToFlush, b.records)
	b.records = b.records[:0] // Clear buffer

	// Update metrics
	b.metrics.FlushOperations++
	b.metrics.LastFlush = start

	totalRows := int64(0)
	for _, record := range recordsToFlush {
		totalRows += record.NumRows()
	}
	b.metrics.RecordsFlushed += totalRows

	// Update average flush size
	if b.metrics.FlushOperations > 0 {
		b.metrics.AverageFlushSize = b.metrics.RecordsFlushed / b.metrics.FlushOperations
	}

	b.logger.WithFields(logrus.Fields{
		"records_count": len(recordsToFlush),
		"total_rows":    totalRows,
		"took":          time.Since(start),
	}).Info("Buffer: Flushing records")

	// Flush to storage via handler
	if b.flushHandler != nil {
		ctx := context.Background()
		if err := b.flushHandler.Flush(ctx, recordsToFlush); err != nil {
			b.logger.WithError(err).Error("Buffer: Flush handler failed")
			b.metrics.ErrorCount++
			// Release records on error
			for _, record := range recordsToFlush {
				record.Release()
			}
			return
		}
	}

	// Release records after successful flush
	for _, record := range recordsToFlush {
		record.Release()
	}

	b.lastFlush = start
}

// GetMetrics returns current buffer metrics
func (b *Buffer) GetMetrics() BufferMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()

	metrics := b.metrics
	metrics.CurrentBufferSize = len(b.records)
	return metrics
}

// Close gracefully shuts down the buffer
func (b *Buffer) Close(ctx context.Context) error {
	b.logger.Info("Buffer: Shutting down")

	// Signal shutdown
	close(b.done)

	// Wait for flush loop to finish
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		b.logger.Info("Buffer: Shutdown complete")
		return nil
	case <-time.After(30 * time.Second):
		b.logger.Warn("Buffer: Shutdown timeout")
		return ctx.Err()
	}
}

// Size returns the current buffer size
func (b *Buffer) Size() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.records)
}

// IsEmpty returns true if the buffer is empty
func (b *Buffer) IsEmpty() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.records) == 0
}

// TimeSinceLastFlush returns the time since the last flush
func (b *Buffer) TimeSinceLastFlush() time.Duration {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return time.Since(b.lastFlush)
}

// ResetMetrics resets the buffer metrics
func (b *Buffer) ResetMetrics() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.metrics = BufferMetrics{
		MaxBufferSize: b.config.BufferSize,
	}
}
