package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/sirupsen/logrus"
)

// WAL (Write-Ahead Log) provides durability guarantees for data ingestion
type WAL struct {
	config      *config.WALConfig
	storage     storage.Backend
	pathBuilder storage.PathBuilder
	logger      *logrus.Logger

	// Runtime state
	mu          sync.RWMutex
	currentFile *os.File
	writer      *bufio.Writer
	filePath    string
	fileSize    int64
	fileCreated time.Time // Track file creation time
	sequence    uint64
	isOpen      bool

	// Metrics
	metrics WALMetrics
}

// WALMetrics holds WAL performance metrics
type WALMetrics struct {
	RecordsWritten   int64     `json:"records_written"`
	BytesWritten     int64     `json:"bytes_written"`
	SyncOperations   int64     `json:"sync_operations"`
	FileRotations    int64     `json:"file_rotations"`
	LastWrite        time.Time `json:"last_write"`
	LastSync         time.Time `json:"last_sync"`
	LastError        time.Time `json:"last_error"`
	ErrorCount       int64     `json:"error_count"`
	AverageWriteSize float64   `json:"average_write_size"`
}

// WALEntry represents a single WAL entry
type WALEntry struct {
	Sequence    uint64            `json:"sequence"`
	Timestamp   time.Time         `json:"timestamp"`
	Database    string            `json:"database"`
	Measurement string            `json:"measurement"`
	Data        []byte            `json:"data"`
	Headers     map[string]string `json:"headers,omitempty"`
}

// WALReader reads entries from WAL files
type WALReader struct {
	config      *config.WALConfig
	storage     storage.Backend
	pathBuilder storage.PathBuilder
	logger      *logrus.Logger
}

// NewWAL creates a new Write-Ahead Log
func NewWAL(
	cfg *config.WALConfig,
	storage storage.Backend,
	pathBuilder storage.PathBuilder,
	logger *logrus.Logger,
) (*WAL, error) {
	if logger == nil {
		logger = logrus.New()
	}

	if !cfg.Enabled {
		logger.Info("WAL is disabled")
		return &WAL{
			config:  cfg,
			storage: storage,
			logger:  logger,
			isOpen:  false,
		}, nil
	}

	wal := &WAL{
		config:      cfg,
		storage:     storage,
		pathBuilder: pathBuilder,
		logger:      logger,
		metrics:     WALMetrics{},
	}

	// Create WAL directory
	if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Open current WAL file
	if err := wal.openCurrentFile(); err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"directory": cfg.Dir,
		"sync_mode": cfg.SyncMode,
		"max_size":  cfg.MaxSizeMB,
		"max_age":   cfg.MaxAgeSeconds,
	}).Info("WAL initialized")

	return wal, nil
}

// NewWALReader creates a new WAL reader
func NewWALReader(
	cfg *config.WALConfig,
	storage storage.Backend,
	pathBuilder storage.PathBuilder,
	logger *logrus.Logger,
) *WALReader {
	if logger == nil {
		logger = logrus.New()
	}

	return &WALReader{
		config:      cfg,
		storage:     storage,
		pathBuilder: pathBuilder,
		logger:      logger,
	}
}

// Write writes an entry to the WAL
func (w *WAL) Write(ctx context.Context, database, measurement string, data []byte, headers map[string]string) error {
	if !w.config.Enabled {
		return nil // WAL disabled, no-op
	}

	start := time.Now()
	defer func() {
		w.metrics.RecordsWritten++
		w.metrics.LastWrite = start

		// Update average write size
		if w.metrics.RecordsWritten > 0 {
			w.metrics.AverageWriteSize = float64(w.metrics.BytesWritten) / float64(w.metrics.RecordsWritten)
		}
	}()

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return fmt.Errorf("WAL is not open")
	}

	// Create entry
	entry := WALEntry{
		Sequence:    w.sequence,
		Timestamp:   start,
		Database:    database,
		Measurement: measurement,
		Data:        data,
		Headers:     headers,
	}

	// Serialize entry
	entryData, err := w.serializeEntry(entry)
	if err != nil {
		w.logError(fmt.Errorf("failed to serialize WAL entry: %w", err))
		return err
	}

	// Write to file
	if err := w.writeToFile(entryData); err != nil {
		w.logError(fmt.Errorf("failed to write WAL entry: %w", err))
		return err
	}

	// Update metrics
	w.metrics.BytesWritten += int64(len(entryData))
	w.sequence++

	// Check if rotation is needed
	if w.shouldRotate() {
		if err := w.rotateFile(); err != nil {
			w.logError(fmt.Errorf("failed to rotate WAL file: %w", err))
			return err
		}
	}

	w.logger.WithFields(logrus.Fields{
		"sequence":    entry.Sequence,
		"database":    database,
		"measurement": measurement,
		"data_size":   len(data),
		"took":        time.Since(start),
	}).Debug("WAL: Entry written")

	return nil
}

// Sync ensures data is persisted to disk
func (w *WAL) Sync() error {
	if !w.config.Enabled || !w.isOpen {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	start := time.Now()
	defer func() {
		w.metrics.SyncOperations++
		w.metrics.LastSync = start
	}()

	// Flush buffer
	if err := w.writer.Flush(); err != nil {
		w.logError(fmt.Errorf("failed to flush WAL buffer: %w", err))
		return err
	}

	// Sync to disk based on sync mode
	switch w.config.SyncMode {
	case "fsync":
		// Full sync: data + metadata (slowest, most durable)
		if err := w.currentFile.Sync(); err != nil {
			w.logError(fmt.Errorf("failed to fsync WAL file: %w", err))
			return err
		}
	case "fdatasync":
		// Data sync only: syncs file data but not metadata (faster than fsync)
		// This is equivalent to Python's os.fdatasync() - syncs writes but not stat metadata
		// Note: fdatasync may not be available on all platforms (e.g., macOS doesn't have it)
		if err := fdatasync(w.currentFile); err != nil {
			w.logError(fmt.Errorf("failed to fdatasync WAL file: %w", err))
			return err
		}
	case "async":
		// No explicit sync, rely on OS buffer cache (fastest, least durable)
	default:
		w.logger.WithField("sync_mode", w.config.SyncMode).Warn("Unknown sync mode, using fsync")
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
	}

	w.logger.WithFields(logrus.Fields{
		"sync_mode": w.config.SyncMode,
		"took":      time.Since(start),
	}).Debug("WAL: Synced")

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.isOpen {
		return nil
	}

	w.logger.Info("WAL: Closing")

	// Final sync
	if err := w.Sync(); err != nil {
		w.logger.WithError(err).Warn("Failed to sync WAL before closing")
	}

	// Close file
	if err := w.writer.Flush(); err != nil {
		w.logger.WithError(err).Warn("Failed to flush WAL buffer before closing")
	}

	if err := w.currentFile.Close(); err != nil {
		w.logger.WithError(err).Error("Failed to close WAL file")
		return err
	}

	w.isOpen = false
	w.currentFile = nil
	w.writer = nil

	w.logger.Info("WAL: Closed")
	return nil
}

// GetMetrics returns WAL metrics
func (w *WAL) GetMetrics() WALMetrics {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.metrics
}

// ResetMetrics resets WAL metrics
func (w *WAL) ResetMetrics() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.metrics = WALMetrics{}
}

// openCurrentFile opens the current WAL file
func (w *WAL) openCurrentFile() error {
	// Find existing WAL files
	files, err := w.findWALFiles()
	if err != nil {
		return err
	}

	var filePath string
	var sequence uint64

	if len(files) == 0 {
		// Create new file
		workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano())
		filePath = w.pathBuilder.BuildWALPath("default", workerID)
		sequence = 0
	} else {
		// Use most recent file
		latestFile := files[len(files)-1]
		filePath = latestFile.Path

		// Read sequence from file header
		seq, err := w.readSequenceFromFile(filePath)
		if err != nil {
			w.logger.WithError(err).WithField("file", filePath).Warn("Failed to read sequence from WAL file, starting from 0")
			sequence = 0
		} else {
			sequence = seq
		}
	}

	// Open file in append mode
	// Add O_DSYNC flag for fdatasync mode to enable hardware-level data sync
	// This matches Python's behavior: writes block until data is on disk
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if w.config.SyncMode == "fdatasync" {
		// O_DSYNC: writes block until data is physically written (not metadata)
		// This is faster than O_SYNC (which also syncs metadata)
		// Platform-specific: getDSyncFlag() returns 0 on platforms without O_DSYNC
		if dsyncFlag := getDSyncFlag(); dsyncFlag != 0 {
			flags |= dsyncFlag
		}
	}

	file, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file %s: %w", filePath, err)
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to stat WAL file %s: %w", filePath, err)
	}

	w.currentFile = file
	w.writer = bufio.NewWriter(file)
	w.filePath = filePath
	w.fileSize = stat.Size()
	w.fileCreated = stat.ModTime() // Use mod time as creation time
	w.sequence = sequence
	w.isOpen = true

	return nil
}

// serializeEntry serializes a WAL entry
func (w *WAL) serializeEntry(entry WALEntry) ([]byte, error) {
	// Simple binary format:
	// 8 bytes: sequence
	// 8 bytes: timestamp (unix nanos)
	// 4 bytes: database length
	// n bytes: database
	// 4 bytes: measurement length
	// n bytes: measurement
	// 4 bytes: data length
	// n bytes: data
	// 4 bytes: headers count
	// for each header:
	//   4 bytes: key length
	//   n bytes: key
	//   4 bytes: value length
	//   n bytes: value

	buf := make([]byte, 0, 1024) // Start with 1KB buffer

	// Add sequence
	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, entry.Sequence)
	buf = append(buf, seqBytes...)

	// Add timestamp
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(entry.Timestamp.UnixNano()))
	buf = append(buf, tsBytes...)

	// Add database
	dbBytes := []byte(entry.Database)
	dbLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dbLen, uint32(len(dbBytes)))
	buf = append(buf, dbLen...)
	buf = append(buf, dbBytes...)

	// Add measurement
	measBytes := []byte(entry.Measurement)
	measLen := make([]byte, 4)
	binary.BigEndian.PutUint32(measLen, uint32(len(measBytes)))
	buf = append(buf, measLen...)
	buf = append(buf, measBytes...)

	// Add data
	dataLen := make([]byte, 4)
	binary.BigEndian.PutUint32(dataLen, uint32(len(entry.Data)))
	buf = append(buf, dataLen...)
	buf = append(buf, entry.Data...)

	// Add headers
	headerCount := make([]byte, 4)
	binary.BigEndian.PutUint32(headerCount, uint32(len(entry.Headers)))
	buf = append(buf, headerCount...)

	for key, value := range entry.Headers {
		// Key
		keyBytes := []byte(key)
		keyLen := make([]byte, 4)
		binary.BigEndian.PutUint32(keyLen, uint32(len(keyBytes)))
		buf = append(buf, keyLen...)
		buf = append(buf, keyBytes...)

		// Value
		valueBytes := []byte(value)
		valueLen := make([]byte, 4)
		binary.BigEndian.PutUint32(valueLen, uint32(len(valueBytes)))
		buf = append(buf, valueLen...)
		buf = append(buf, valueBytes...)
	}

	return buf, nil
}

// writeToFile writes data to the current WAL file
func (w *WAL) writeToFile(data []byte) error {
	n, err := w.writer.Write(data)
	if err != nil {
		return err
	}

	w.fileSize += int64(n)
	return nil
}

// shouldRotate checks if the WAL file should be rotated
func (w *WAL) shouldRotate() bool {
	maxSize := int64(w.config.MaxSizeMB) * 1024 * 1024
	maxAge := time.Duration(w.config.MaxAgeSeconds) * time.Second

	// Check size
	if w.fileSize >= maxSize {
		w.logger.WithFields(logrus.Fields{
			"file_size": w.fileSize,
			"max_size":  maxSize,
		}).Debug("WAL: File size threshold reached")
		return true
	}

	// Check age
	if maxAge > 0 && time.Since(w.fileCreated) >= maxAge {
		w.logger.WithFields(logrus.Fields{
			"file_age": time.Since(w.fileCreated),
			"max_age":  maxAge,
		}).Debug("WAL: File age threshold reached")
		return true
	}

	return false
}

// rotateFile rotates the current WAL file
func (w *WAL) rotateFile() error {
	w.logger.Info("WAL: Rotating file")

	// Close current file
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.currentFile.Close(); err != nil {
		return err
	}

	// Create new file
	workerID := fmt.Sprintf("worker-%d", time.Now().UnixNano())
	newFilePath := w.pathBuilder.BuildWALPath("default", workerID)

	// Open file with same flags as openCurrentFile
	flags := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if w.config.SyncMode == "fdatasync" {
		if dsyncFlag := getDSyncFlag(); dsyncFlag != 0 {
			flags |= dsyncFlag
		}
	}

	file, err := os.OpenFile(newFilePath, flags, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new WAL file %s: %w", newFilePath, err)
	}

	w.currentFile = file
	w.writer = bufio.NewWriter(file)
	w.filePath = newFilePath
	w.fileSize = 0
	w.fileCreated = time.Now()
	w.sequence = 0

	w.metrics.FileRotations++

	w.logger.WithField("new_file", newFilePath).Info("WAL: File rotated")
	return nil
}

// findWALFiles finds existing WAL files
func (w *WAL) findWALFiles() ([]WALFileInfo, error) {
	pattern := filepath.Join(w.config.Dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	files := make([]WALFileInfo, 0, len(matches))
	for _, match := range matches {
		stat, err := os.Stat(match)
		if err != nil {
			continue
		}

		files = append(files, WALFileInfo{
			Path:    match,
			Size:    stat.Size(),
			ModTime: stat.ModTime(),
		})
	}

	// Sort by modification time
	// For simplicity, we'll just return them as-is

	return files, nil
}

// readSequenceFromFile reads the sequence number from a WAL file
func (w *WAL) readSequenceFromFile(filePath string) (uint64, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Read first 8 bytes (sequence)
	seqBytes := make([]byte, 8)
	n, err := file.Read(seqBytes)
	if err != nil || n != 8 {
		return 0, fmt.Errorf("failed to read sequence from WAL file")
	}

	sequence := binary.BigEndian.Uint64(seqBytes)
	return sequence, nil
}

// logError logs an error and updates metrics
func (w *WAL) logError(err error) {
	w.metrics.ErrorCount++
	w.metrics.LastError = time.Now()
	w.logger.WithError(err).Error("WAL error")
}

// WALFileInfo represents information about a WAL file
type WALFileInfo struct {
	Path    string    `json:"path"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

// RecoveryHandler handles WAL entry recovery
type RecoveryHandler interface {
	RecoverEntry(ctx context.Context, entry WALEntry) error
}

// Recover replays all WAL entries through the recovery handler
func (w *WAL) Recover(ctx context.Context, handler RecoveryHandler) error {
	if !w.config.Enabled {
		w.logger.Info("WAL is disabled, skipping recovery")
		return nil
	}

	w.logger.Info("WAL: Starting recovery")
	start := time.Now()

	// Find all WAL files
	files, err := w.findWALFiles()
	if err != nil {
		return fmt.Errorf("failed to find WAL files: %w", err)
	}

	if len(files) == 0 {
		w.logger.Info("WAL: No files to recover")
		return nil
	}

	w.logger.WithField("file_count", len(files)).Info("WAL: Found files for recovery")

	// Create reader
	reader := NewWALReader(w.config, w.storage, w.pathBuilder, w.logger)

	// Read and replay entries
	entryCh, err := reader.ReadEntries(ctx, "", "", time.Time{}) // Read all entries
	if err != nil {
		return fmt.Errorf("failed to read WAL entries: %w", err)
	}

	recovered := 0
	failed := 0

	for entry := range entryCh {
		if err := handler.RecoverEntry(ctx, entry); err != nil {
			w.logger.WithError(err).WithFields(logrus.Fields{
				"sequence":    entry.Sequence,
				"database":    entry.Database,
				"measurement": entry.Measurement,
			}).Error("WAL: Failed to recover entry")
			failed++
		} else {
			recovered++
		}
	}

	w.logger.WithFields(logrus.Fields{
		"recovered": recovered,
		"failed":    failed,
		"took":      time.Since(start),
	}).Info("WAL: Recovery complete")

	return nil
}

// Cleanup removes old WAL files based on age and count
func (w *WAL) Cleanup(ctx context.Context, maxAgeSeconds int, maxFiles int) error {
	if !w.config.Enabled {
		return nil
	}

	w.logger.Debug("WAL: Starting cleanup")
	start := time.Now()

	files, err := w.findWALFiles()
	if err != nil {
		return fmt.Errorf("failed to find WAL files: %w", err)
	}

	if len(files) == 0 {
		return nil
	}

	now := time.Now()
	maxAge := time.Duration(maxAgeSeconds) * time.Second
	toDelete := make([]string, 0)

	// Sort files by modification time (oldest first)
	sortedFiles := files

	// Delete files based on age
	for _, file := range sortedFiles {
		if now.Sub(file.ModTime) > maxAge {
			// Don't delete the current file
			if file.Path != w.filePath {
				toDelete = append(toDelete, file.Path)
			}
		}
	}

	// Delete excess files if we have more than maxFiles
	if maxFiles > 0 && len(sortedFiles) > maxFiles {
		excessCount := len(sortedFiles) - maxFiles
		for i := 0; i < excessCount && i < len(sortedFiles); i++ {
			if sortedFiles[i].Path != w.filePath {
				// Check if already in toDelete
				found := false
				for _, path := range toDelete {
					if path == sortedFiles[i].Path {
						found = true
						break
					}
				}
				if !found {
					toDelete = append(toDelete, sortedFiles[i].Path)
				}
			}
		}
	}

	// Delete files
	deleted := 0
	for _, path := range toDelete {
		if err := os.Remove(path); err != nil {
			w.logger.WithError(err).WithField("file", path).Warn("WAL: Failed to delete old file")
		} else {
			deleted++
			w.logger.WithField("file", path).Debug("WAL: Deleted old file")
		}
	}

	w.logger.WithFields(logrus.Fields{
		"deleted": deleted,
		"took":    time.Since(start),
	}).Info("WAL: Cleanup complete")

	return nil
}

// Truncate removes all WAL entries up to a certain sequence number
func (w *WAL) Truncate(ctx context.Context, upToSequence uint64) error {
	if !w.config.Enabled {
		return nil
	}

	w.logger.WithField("sequence", upToSequence).Info("WAL: Truncating")

	// For simplicity, we'll just clean up old files
	// A more sophisticated implementation would rewrite files
	return w.Cleanup(ctx, w.config.MaxAgeSeconds, 0)
}

// ReadEntries reads WAL entries from files
func (r *WALReader) ReadEntries(ctx context.Context, database, measurement string, since time.Time) (<-chan WALEntry, error) {
	if !r.config.Enabled {
		return nil, fmt.Errorf("WAL is disabled")
	}

	entryCh := make(chan WALEntry, 100) // Buffered channel

	go func() {
		defer close(entryCh)

		// Find WAL files
		files, err := r.findWALFiles()
		if err != nil {
			r.logger.WithError(err).Error("Failed to find WAL files")
			return
		}

		// Process files
		for _, file := range files {
			if file.ModTime.Before(since) {
				continue
			}

			r.readFile(ctx, file.Path, database, measurement, entryCh)
		}
	}()

	return entryCh, nil
}

// findWALFiles finds existing WAL files (implementation similar to WAL.findWALFiles)
func (r *WALReader) findWALFiles() ([]WALFileInfo, error) {
	pattern := filepath.Join(r.config.Dir, "*.wal")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	files := make([]WALFileInfo, 0, len(matches))
	for _, match := range matches {
		stat, err := os.Stat(match)
		if err != nil {
			continue
		}

		files = append(files, WALFileInfo{
			Path:    match,
			Size:    stat.Size(),
			ModTime: stat.ModTime(),
		})
	}

	return files, nil
}

// readFile reads entries from a single WAL file
func (r *WALReader) readFile(ctx context.Context, filePath, database, measurement string, entryCh chan<- WALEntry) {
	file, err := os.Open(filePath)
	if err != nil {
		r.logger.WithError(err).WithField("file", filePath).Error("Failed to open WAL file")
		return
	}
	defer file.Close()

	r.logger.WithField("file", filePath).Debug("Reading WAL file")

	reader := bufio.NewReader(file)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entry, err := r.deserializeEntry(reader)
		if err != nil {
			if err.Error() != "EOF" {
				r.logger.WithError(err).WithField("file", filePath).Warn("Failed to read WAL entry")
			}
			return
		}

		// Filter by database and measurement if specified
		if database != "" && entry.Database != database {
			continue
		}
		if measurement != "" && entry.Measurement != measurement {
			continue
		}

		entryCh <- *entry
	}
}

// deserializeEntry deserializes a WAL entry from a reader
func (r *WALReader) deserializeEntry(reader *bufio.Reader) (*WALEntry, error) {
	// Read sequence (8 bytes)
	seqBytes := make([]byte, 8)
	if _, err := reader.Read(seqBytes); err != nil {
		return nil, err
	}
	sequence := binary.BigEndian.Uint64(seqBytes)

	// Read timestamp (8 bytes)
	tsBytes := make([]byte, 8)
	if _, err := reader.Read(tsBytes); err != nil {
		return nil, err
	}
	timestamp := time.Unix(0, int64(binary.BigEndian.Uint64(tsBytes)))

	// Read database
	database, err := r.readString(reader)
	if err != nil {
		return nil, err
	}

	// Read measurement
	measurement, err := r.readString(reader)
	if err != nil {
		return nil, err
	}

	// Read data
	data, err := r.readBytes(reader)
	if err != nil {
		return nil, err
	}

	// Read headers count (4 bytes)
	headerCountBytes := make([]byte, 4)
	if _, err := reader.Read(headerCountBytes); err != nil {
		return nil, err
	}
	headerCount := binary.BigEndian.Uint32(headerCountBytes)

	// Read headers
	headers := make(map[string]string)
	for i := uint32(0); i < headerCount; i++ {
		key, err := r.readString(reader)
		if err != nil {
			return nil, err
		}

		value, err := r.readString(reader)
		if err != nil {
			return nil, err
		}

		headers[key] = value
	}

	return &WALEntry{
		Sequence:    sequence,
		Timestamp:   timestamp,
		Database:    database,
		Measurement: measurement,
		Data:        data,
		Headers:     headers,
	}, nil
}

// readString reads a length-prefixed string
func (r *WALReader) readString(reader *bufio.Reader) (string, error) {
	lenBytes := make([]byte, 4)
	if _, err := reader.Read(lenBytes); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(lenBytes)

	strBytes := make([]byte, length)
	if _, err := reader.Read(strBytes); err != nil {
		return "", err
	}

	return string(strBytes), nil
}

// readBytes reads a length-prefixed byte slice
func (r *WALReader) readBytes(reader *bufio.Reader) ([]byte, error) {
	lenBytes := make([]byte, 4)
	if _, err := reader.Read(lenBytes); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBytes)

	data := make([]byte, length)
	if _, err := reader.Read(data); err != nil {
		return nil, err
	}

	return data, nil
}
