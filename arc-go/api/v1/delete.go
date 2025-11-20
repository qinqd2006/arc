package v1

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/arc-core/arc-go/internal/config"
	storagelib "github.com/arc-core/arc-go/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// DeleteHandler handles data deletion endpoints
type DeleteHandler struct {
	config      *config.Config
	storage     storagelib.Backend
	pathBuilder storagelib.PathBuilder
	logger      *logrus.Logger
}

// NewDeleteHandler creates a new delete handler
func NewDeleteHandler(
	cfg *config.Config,
	storage storagelib.Backend,
	logger *logrus.Logger,
) *DeleteHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &DeleteHandler{
		config:      cfg,
		storage:     storage,
		pathBuilder: storagelib.NewDefaultPathBuilder(),
		logger:      logger,
	}
}

// DeleteRequest represents a delete request
type DeleteRequest struct {
	Database    string            `json:"database" binding:"required"`
	Measurement string            `json:"measurement,omitempty"`
	StartTime   time.Time         `json:"start_time,omitempty"`
	EndTime     time.Time         `json:"end_time,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
	DryRun      bool              `json:"dry_run"`
}

// DeleteResponse represents a delete response
type DeleteResponse struct {
	Status        string   `json:"status"`
	FilesDeleted  int      `json:"files_deleted"`
	BytesDeleted  int64    `json:"bytes_deleted"`
	FilesAffected []string `json:"files_affected,omitempty"`
	Duration      string   `json:"duration"`
	Message       string   `json:"message,omitempty"`
}

// Delete deletes time-series data based on the request criteria
func (h *DeleteHandler) Delete(c *gin.Context) {
	var req DeleteRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request body",
			"details": err.Error(),
		})
		return
	}

	start := time.Now()

	h.logger.WithFields(logrus.Fields{
		"database":    req.Database,
		"measurement": req.Measurement,
		"start_time":  req.StartTime,
		"end_time":    req.EndTime,
		"tags":        req.Tags,
		"dry_run":     req.DryRun,
	}).Info("Delete request received")

	// Find files to delete
	ctx := context.Background()
	files, err := h.findFilesToDelete(ctx, req)
	if err != nil {
		h.logger.WithError(err).Error("Failed to find files to delete")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to find files to delete",
			"details": err.Error(),
		})
		return
	}

	if len(files) == 0 {
		c.JSON(http.StatusOK, DeleteResponse{
			Status:       "success",
			FilesDeleted: 0,
			BytesDeleted: 0,
			Duration:     time.Since(start).String(),
			Message:      "No files match the delete criteria",
		})
		return
	}

	// Calculate total size
	totalSize := int64(0)
	fileList := make([]string, len(files))
	for i, file := range files {
		totalSize += file.Size
		fileList[i] = file.Key
	}

	// If dry run, just return what would be deleted
	if req.DryRun {
		c.JSON(http.StatusOK, DeleteResponse{
			Status:        "success",
			FilesDeleted:  0,
			BytesDeleted:  0,
			FilesAffected: fileList,
			Duration:      time.Since(start).String(),
			Message:       fmt.Sprintf("Dry run: would delete %d files (%d bytes)", len(files), totalSize),
		})
		return
	}

	// Delete files
	deletedCount, err := h.deleteFiles(ctx, fileList)
	if err != nil {
		h.logger.WithError(err).Error("Failed to delete files")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":         "Failed to delete some files",
			"details":       err.Error(),
			"files_deleted": deletedCount,
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"files_deleted": deletedCount,
		"bytes_deleted": totalSize,
		"duration":      time.Since(start),
	}).Info("Delete operation completed")

	c.JSON(http.StatusOK, DeleteResponse{
		Status:       "success",
		FilesDeleted: deletedCount,
		BytesDeleted: totalSize,
		Duration:     time.Since(start).String(),
		Message:      fmt.Sprintf("Successfully deleted %d files", deletedCount),
	})
}

// DeleteDatabase deletes an entire database
func (h *DeleteHandler) DeleteDatabase(c *gin.Context) {
	database := c.Param("database")
	if database == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Database name is required",
		})
		return
	}

	dryRun := c.Query("dry_run") == "true"

	start := time.Now()

	h.logger.WithFields(logrus.Fields{
		"database": database,
		"dry_run":  dryRun,
	}).Info("Delete database request received")

	ctx := context.Background()

	// List all files in the database
	prefix := h.pathBuilder.BuildDatabasePrefix(database)
	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		h.logger.WithError(err).Error("Failed to list database files")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to list database files",
			"details": err.Error(),
		})
		return
	}

	if len(files) == 0 {
		c.JSON(http.StatusOK, DeleteResponse{
			Status:       "success",
			FilesDeleted: 0,
			BytesDeleted: 0,
			Duration:     time.Since(start).String(),
			Message:      "Database not found or already empty",
		})
		return
	}

	// Calculate total size
	totalSize := int64(0)
	fileList := make([]string, len(files))
	for i, file := range files {
		totalSize += file.Size
		fileList[i] = file.Key
	}

	// If dry run, just return what would be deleted
	if dryRun {
		c.JSON(http.StatusOK, DeleteResponse{
			Status:        "success",
			FilesDeleted:  0,
			BytesDeleted:  0,
			FilesAffected: fileList,
			Duration:      time.Since(start).String(),
			Message:       fmt.Sprintf("Dry run: would delete database %s (%d files, %d bytes)", database, len(files), totalSize),
		})
		return
	}

	// Delete files
	deletedCount, err := h.deleteFiles(ctx, fileList)
	if err != nil {
		h.logger.WithError(err).Error("Failed to delete database files")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":         "Failed to delete some files",
			"details":       err.Error(),
			"files_deleted": deletedCount,
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"database":      database,
		"files_deleted": deletedCount,
		"bytes_deleted": totalSize,
		"duration":      time.Since(start),
	}).Info("Database deleted successfully")

	c.JSON(http.StatusOK, DeleteResponse{
		Status:       "success",
		FilesDeleted: deletedCount,
		BytesDeleted: totalSize,
		Duration:     time.Since(start).String(),
		Message:      fmt.Sprintf("Successfully deleted database %s (%d files)", database, deletedCount),
	})
}

// DeleteMeasurement deletes a measurement from a database
func (h *DeleteHandler) DeleteMeasurement(c *gin.Context) {
	database := c.Param("database")
	measurement := c.Param("measurement")

	if database == "" || measurement == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Database and measurement names are required",
		})
		return
	}

	dryRun := c.Query("dry_run") == "true"

	start := time.Now()

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"measurement": measurement,
		"dry_run":     dryRun,
	}).Info("Delete measurement request received")

	ctx := context.Background()

	// List all files in the measurement
	prefix := h.pathBuilder.BuildMeasurementPrefix(database, measurement)
	files, err := h.storage.List(ctx, prefix)
	if err != nil {
		h.logger.WithError(err).Error("Failed to list measurement files")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to list measurement files",
			"details": err.Error(),
		})
		return
	}

	if len(files) == 0 {
		c.JSON(http.StatusOK, DeleteResponse{
			Status:       "success",
			FilesDeleted: 0,
			BytesDeleted: 0,
			Duration:     time.Since(start).String(),
			Message:      "Measurement not found or already empty",
		})
		return
	}

	// Calculate total size
	totalSize := int64(0)
	fileList := make([]string, len(files))
	for i, file := range files {
		totalSize += file.Size
		fileList[i] = file.Key
	}

	// If dry run, just return what would be deleted
	if dryRun {
		c.JSON(http.StatusOK, DeleteResponse{
			Status:        "success",
			FilesDeleted:  0,
			BytesDeleted:  0,
			FilesAffected: fileList,
			Duration:      time.Since(start).String(),
			Message:       fmt.Sprintf("Dry run: would delete measurement %s/%s (%d files, %d bytes)", database, measurement, len(files), totalSize),
		})
		return
	}

	// Delete files
	deletedCount, err := h.deleteFiles(ctx, fileList)
	if err != nil {
		h.logger.WithError(err).Error("Failed to delete measurement files")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":         "Failed to delete some files",
			"details":       err.Error(),
			"files_deleted": deletedCount,
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"database":      database,
		"measurement":   measurement,
		"files_deleted": deletedCount,
		"bytes_deleted": totalSize,
		"duration":      time.Since(start),
	}).Info("Measurement deleted successfully")

	c.JSON(http.StatusOK, DeleteResponse{
		Status:       "success",
		FilesDeleted: deletedCount,
		BytesDeleted: totalSize,
		Duration:     time.Since(start).String(),
		Message:      fmt.Sprintf("Successfully deleted measurement %s/%s (%d files)", database, measurement, deletedCount),
	})
}

// findFilesToDelete finds files matching the delete criteria
func (h *DeleteHandler) findFilesToDelete(ctx context.Context, req DeleteRequest) ([]storagelib.ObjectInfo, error) {
	var prefix string

	if req.Measurement != "" {
		// Search within a specific measurement
		prefix = h.pathBuilder.BuildMeasurementPrefix(req.Database, req.Measurement)
	} else {
		// Search entire database
		prefix = h.pathBuilder.BuildDatabasePrefix(req.Database)
	}

	// List all files
	allFiles, err := h.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	// Filter files based on time range
	filteredFiles := make([]storagelib.ObjectInfo, 0)
	for _, file := range allFiles {
		// Skip system files
		if h.pathBuilder.IsSystemPath(file.Key) {
			continue
		}

		// Only consider Parquet files
		if !strings.HasSuffix(file.Key, ".parquet") {
			continue
		}

		// If time range is specified, check if file falls within range
		if !req.StartTime.IsZero() || !req.EndTime.IsZero() {
			fileTime, err := h.getFileTime(file.Key)
			if err != nil {
				h.logger.WithError(err).WithField("file", file.Key).Warn("Failed to parse file time, skipping")
				continue
			}

			if !req.StartTime.IsZero() && fileTime.Before(req.StartTime) {
				continue
			}

			if !req.EndTime.IsZero() && fileTime.After(req.EndTime) {
				continue
			}
		}

		filteredFiles = append(filteredFiles, file)
	}

	return filteredFiles, nil
}

// getFileTime extracts the time from a file path
func (h *DeleteHandler) getFileTime(path string) (time.Time, error) {
	return h.pathBuilder.ParseTimeFromPath(path)
}

// deleteFiles deletes a list of files
func (h *DeleteHandler) deleteFiles(ctx context.Context, files []string) (int, error) {
	if len(files) == 0 {
		return 0, nil
	}

	// Try batch delete first
	err := h.storage.DeleteBatch(ctx, files)
	if err == nil {
		return len(files), nil
	}

	// Fall back to individual deletes
	h.logger.WithError(err).Warn("Batch delete failed, falling back to individual deletes")

	deletedCount := 0
	var lastErr error

	for _, file := range files {
		if err := h.storage.Delete(ctx, file); err != nil {
			h.logger.WithError(err).WithField("file", file).Error("Failed to delete file")
			lastErr = err
		} else {
			deletedCount++
		}
	}

	if lastErr != nil && deletedCount < len(files) {
		return deletedCount, fmt.Errorf("failed to delete some files: %w", lastErr)
	}

	return deletedCount, nil
}

// Health checks delete handler health
func (h *DeleteHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

// Close gracefully shuts down the delete handler
func (h *DeleteHandler) Close(ctx context.Context) error {
	h.logger.Info("DeleteHandler: Shutdown complete")
	return nil
}
