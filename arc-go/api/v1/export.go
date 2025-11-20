package v1

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/exporter"
	"github.com/arc-core/arc-go/internal/query"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// ExportHandler handles data export endpoints
type ExportHandler struct {
	config         *config.Config
	exporterConfig *exporter.ExporterConfig
	exporter       *exporter.Exporter
	queryEngine    *query.DuckDBEngine
	storage        storage.Backend
	logger         *logrus.Logger
}

// NewExportHandler creates a new export handler
func NewExportHandler(
	cfg *config.Config,
	queryEngine *query.DuckDBEngine,
	storage storage.Backend,
	logger *logrus.Logger,
) *ExportHandler {
	if logger == nil {
		logger = logrus.New()
	}

	// Create exporter configuration with default values
	// TODO: Add Exporter section to config file when complete
	exporterConfig := &exporter.ExporterConfig{
		Enabled:       true,
		BatchSize:     10000,
		FlushInterval: 30 * time.Second,
		MaxRetries:    3,
		RetryDelay:    5 * time.Second,
	}

	// Create exporter
	exp := exporter.NewExporter(exporterConfig, queryEngine, storage, logger)

	return &ExportHandler{
		config:         cfg,
		exporterConfig: exporterConfig,
		exporter:       exp,
		queryEngine:    queryEngine,
		storage:        storage,
		logger:         logger,
	}
}

// ExportRequest represents an export API request
type ExportRequest struct {
	Database     string                     `json:"database" binding:"required"`
	Measurements []string                   `json:"measurements,omitempty"`
	StartTime    time.Time                  `json:"start_time" binding:"required"`
	EndTime      time.Time                  `json:"end_time" binding:"required"`
	Format       string                     `json:"format" binding:"required"`
	Destination  exporter.ExportDestination `json:"destination" binding:"required"`
	Filters      map[string]interface{}     `json:"filters,omitempty"`
	Fields       []string                   `json:"fields,omitempty"`
	Limit        int64                      `json:"limit,omitempty"`
}

// ExportResponse represents an export API response
type ExportResponse struct {
	ExportID     string                  `json:"export_id"`
	Status       string                  `json:"status"`
	RecordsCount int64                   `json:"records_count"`
	BytesWritten int64                   `json:"bytes_written"`
	FilesCreated []string                `json:"files_created,omitempty"`
	ErrorMessage string                  `json:"error_message,omitempty"`
	Metadata     exporter.ExportMetadata `json:"metadata"`
}

// ExportProgressResponse represents export progress response
type ExportProgressResponse struct {
	ExportID       string    `json:"export_id"`
	Status         string    `json:"status"`
	Progress       float64   `json:"progress"`
	RecordsCount   int64     `json:"records_count"`
	EstimatedTotal int64     `json:"estimated_total,omitempty"`
	CurrentFile    string    `json:"current_file,omitempty"`
	Error          string    `json:"error,omitempty"`
	LastUpdate     time.Time `json:"last_update"`
}

// Export initiates a data export operation
func (h *ExportHandler) Export(c *gin.Context) {
	start := time.Now()

	// Parse request
	var req ExportRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.WithError(err).Error("Invalid export request")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	// Validate request
	if err := h.validateExportRequest(&req); err != nil {
		h.logger.WithError(err).Error("Invalid export request parameters")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request parameters",
			"details": err.Error(),
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"database":     req.Database,
		"measurements": req.Measurements,
		"start_time":   req.StartTime,
		"end_time":     req.EndTime,
		"format":       req.Format,
		"destination":  req.Destination.Type,
		"remote_addr":  c.ClientIP(),
	}).Debug("Processing export request")

	// Convert to exporter request
	exportReq := &exporter.ExportRequest{
		Database:     req.Database,
		Measurements: req.Measurements,
		StartTime:    req.StartTime,
		EndTime:      req.EndTime,
		Format:       req.Format,
		Destination:  req.Destination,
		Filters:      req.Filters,
		Fields:       req.Fields,
		Limit:        req.Limit,
	}

	// Execute export
	ctx := c.Request.Context()
	result, err := h.exporter.Export(ctx, exportReq)
	if err != nil {
		h.logger.WithError(err).Error("Export operation failed")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Export operation failed",
			"details": err.Error(),
		})
		return
	}

	// Convert to API response
	response := ExportResponse{
		ExportID:     result.ExportID,
		Status:       result.Status,
		RecordsCount: result.RecordsCount,
		BytesWritten: result.BytesWritten,
		FilesCreated: result.FilesCreated,
		ErrorMessage: result.ErrorMessage,
		Metadata:     result.Metadata,
	}

	h.logger.WithFields(logrus.Fields{
		"export_id":     result.ExportID,
		"status":        result.Status,
		"records_count": result.RecordsCount,
		"bytes_written": result.BytesWritten,
		"took":          time.Since(start),
	}).Info("Export request completed")

	c.JSON(http.StatusOK, response)
}

// GetExportProgress returns the progress of an export operation
func (h *ExportHandler) GetExportProgress(c *gin.Context) {
	exportID := c.Param("export_id")
	if exportID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Export ID is required",
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"export_id":   exportID,
		"remote_addr": c.ClientIP(),
	}).Debug("Getting export progress")

	ctx := c.Request.Context()
	progress, err := h.exporter.ExportProgress(ctx, exportID)
	if err != nil {
		h.logger.WithError(err).WithField("export_id", exportID).Error("Failed to get export progress")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to get export progress",
			"details": err.Error(),
		})
		return
	}

	// Convert to API response
	response := ExportProgressResponse{
		ExportID:       progress.ExportID,
		Status:         progress.Status,
		Progress:       progress.Progress,
		RecordsCount:   progress.RecordsCount,
		EstimatedTotal: progress.EstimatedTotal,
		CurrentFile:    progress.CurrentFile,
		Error:          progress.Error,
		LastUpdate:     progress.LastUpdate,
	}

	c.JSON(http.StatusOK, response)
}

// ListExports returns a list of export operations
func (h *ExportHandler) ListExports(c *gin.Context) {
	// Parse query parameters
	database := c.Query("database")
	limitStr := c.DefaultQuery("limit", "50")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000 // Maximum limit
	}

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"limit":       limit,
		"remote_addr": c.ClientIP(),
	}).Debug("Listing exports")

	ctx := c.Request.Context()
	exports, err := h.exporter.ListExports(ctx, database, limit)
	if err != nil {
		h.logger.WithError(err).Error("Failed to list exports")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to list exports",
			"details": err.Error(),
		})
		return
	}

	// Convert to API response
	var response []ExportResponse
	for _, exp := range exports {
		response = append(response, ExportResponse{
			ExportID:     exp.ExportID,
			Status:       exp.Status,
			RecordsCount: exp.RecordsCount,
			BytesWritten: exp.BytesWritten,
			FilesCreated: exp.FilesCreated,
			ErrorMessage: exp.ErrorMessage,
			Metadata:     exp.Metadata,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"exports": response,
		"count":   len(response),
	})
}

// CancelExport cancels an ongoing export operation
func (h *ExportHandler) CancelExport(c *gin.Context) {
	exportID := c.Param("export_id")
	if exportID == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Export ID is required",
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"export_id":   exportID,
		"remote_addr": c.ClientIP(),
	}).Info("Cancelling export")

	ctx := c.Request.Context()
	err := h.exporter.CancelExport(ctx, exportID)
	if err != nil {
		h.logger.WithError(err).WithField("export_id", exportID).Error("Failed to cancel export")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to cancel export",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":   "Export cancellation requested",
		"export_id": exportID,
	})
}

// GetExportDestinations returns supported export destinations
func (h *ExportHandler) GetExportDestinations(c *gin.Context) {
	destinations := []gin.H{
		{
			"type":        "influx1x",
			"name":        "InfluxDB 1.x",
			"description": "Export to InfluxDB 1.x time series database",
			"config_fields": []gin.H{
				{"name": "host", "type": "string", "required": true},
				{"name": "port", "type": "integer", "required": true},
				{"name": "database", "type": "string", "required": true},
				{"name": "username", "type": "string", "required": false},
				{"name": "password", "type": "string", "required": false},
				{"name": "retention_policy", "type": "string", "required": false},
			},
		},
		{
			"type":        "influx2",
			"name":        "InfluxDB 2.x",
			"description": "Export to InfluxDB 2.x time series platform",
			"config_fields": []gin.H{
				{"name": "server_url", "type": "string", "required": true},
				{"name": "token", "type": "string", "required": true},
				{"name": "org", "type": "string", "required": true},
				{"name": "bucket", "type": "string", "required": true},
			},
		},
		{
			"type":        "timescale",
			"name":        "TimescaleDB",
			"description": "Export to TimescaleDB/PostgreSQL database",
			"config_fields": []gin.H{
				{"name": "host", "type": "string", "required": true},
				{"name": "port", "type": "integer", "required": true},
				{"name": "database", "type": "string", "required": true},
				{"name": "username", "type": "string", "required": false},
				{"name": "password", "type": "string", "required": false},
				{"name": "table", "type": "string", "required": false},
			},
		},
		{
			"type":        "http_json",
			"name":        "HTTP JSON API",
			"description": "Export to HTTP JSON endpoint",
			"config_fields": []gin.H{
				{"name": "url", "type": "string", "required": true},
				{"name": "method", "type": "string", "required": false},
				{"name": "headers", "type": "object", "required": false},
				{"name": "timeout", "type": "duration", "required": false},
			},
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"destinations": destinations,
	})
}

// GetExportFormats returns supported export formats
func (h *ExportHandler) GetExportFormats(c *gin.Context) {
	formats := []gin.H{
		{
			"name":        "json",
			"description": "JSON format with structured data",
			"mime_type":   "application/json",
		},
		{
			"name":        "csv",
			"description": "CSV format with comma-separated values",
			"mime_type":   "text/csv",
		},
		{
			"name":        "parquet",
			"description": "Apache Parquet columnar format",
			"mime_type":   "application/octet-stream",
		},
	}

	c.JSON(http.StatusOK, gin.H{
		"formats": formats,
	})
}

// GetExportStatus returns the status of the export system
func (h *ExportHandler) GetExportStatus(c *gin.Context) {
	// Check if exporter is enabled
	status := gin.H{
		"enabled": h.exporterConfig.Enabled,
	}

	if h.exporterConfig.Enabled {
		status["config"] = gin.H{
			"batch_size":     h.exporterConfig.BatchSize,
			"flush_interval": h.exporterConfig.FlushInterval.Seconds(),
			"max_retries":    h.exporterConfig.MaxRetries,
			"retry_delay":    h.exporterConfig.RetryDelay.Seconds(),
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"exporter": status,
	})
}

// validateExportRequest validates an export request
func (h *ExportHandler) validateExportRequest(req *ExportRequest) error {
	// Validate time range
	if req.EndTime.Before(req.StartTime) {
		return fmt.Errorf("end_time must be after start_time")
	}

	// Validate time range duration (max 30 days)
	maxDuration := 30 * 24 * time.Hour
	if req.EndTime.Sub(req.StartTime) > maxDuration {
		return fmt.Errorf("time range cannot exceed 30 days")
	}

	// Validate format
	validFormats := []string{"json", "csv", "parquet"}
	formatValid := false
	for _, f := range validFormats {
		if req.Format == f {
			formatValid = true
			break
		}
	}
	if !formatValid {
		return fmt.Errorf("invalid format: %s. Supported formats: %v", req.Format, validFormats)
	}

	// Validate destination type
	validDestinations := []string{"influx1x", "influx2", "timescale", "http_json"}
	destValid := false
	for _, d := range validDestinations {
		if req.Destination.Type == d {
			destValid = true
			break
		}
	}
	if !destValid {
		return fmt.Errorf("invalid destination type: %s. Supported types: %v", req.Destination.Type, validDestinations)
	}

	// Validate limit
	if req.Limit < 0 {
		return fmt.Errorf("limit cannot be negative")
	}
	if req.Limit > 10000000 { // 10 million records max
		return fmt.Errorf("limit cannot exceed 10 million records")
	}

	return nil
}

// Close closes the export handler
func (h *ExportHandler) Close() error {
	h.logger.Info("ExportHandler: Shutting down")
	return h.exporter.Close()
}
