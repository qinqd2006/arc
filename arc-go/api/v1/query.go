package v1

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/arc-core/arc-go/internal/cache"
	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/arc-core/arc-go/internal/query"
	storagelib "github.com/arc-core/arc-go/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// QueryHandler handles query endpoints
type QueryHandler struct {
	config      *config.Config
	queryEngine *query.DuckDBEngine
	storage     storagelib.Backend
	cache       *cache.QueryCache
	dbManager   *database.Manager
	logger      *logrus.Logger
}

// NewQueryHandler creates a new query handler
func NewQueryHandler(
	cfg *config.Config,
	storage storagelib.Backend,
	logger *logrus.Logger,
) *QueryHandler {
	if logger == nil {
		logger = logrus.New()
	}

	// Create DuckDB engine
	duckdbConfig := query.DuckDBConfig{
		PoolSize:          cfg.DuckDB.PoolSize,
		MaxQueueSize:      cfg.DuckDB.MaxQueueSize,
		MemoryLimit:       cfg.DuckDB.MemoryLimit,
		Threads:           cfg.DuckDB.Threads,
		TempDirectory:     cfg.DuckDB.TempDirectory,
		EnableObjectCache: cfg.DuckDB.EnableObjectCache,
	}

	queryEngine, err := query.NewDuckDBEngine(duckdbConfig, storage, storagelib.NewDefaultPathBuilder(), logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create DuckDB engine")
	}

	// Create query cache
	cacheConfig := cache.CacheConfig{
		Enabled:         cfg.QueryCache.Enabled,
		MaxSizeMB:       cfg.QueryCache.MaxSizeMB,
		TTLSeconds:      cfg.QueryCache.TTLSeconds,
		CleanupInterval: time.Duration(cfg.QueryCache.CleanupIntervalSeconds) * time.Second,
		MaxEntries:      cfg.QueryCache.MaxEntries,
	}
	queryCache := cache.NewQueryCache(cacheConfig, logger)

	return &QueryHandler{
		config:      cfg,
		queryEngine: queryEngine,
		storage:     storage,
		cache:       queryCache,
		logger:      logger,
	}
}

// NewQueryHandlerWithEngine creates a new query handler with a pre-existing query engine
func NewQueryHandlerWithEngine(
	cfg *config.Config,
	storage storagelib.Backend,
	queryEngine *query.DuckDBEngine,
	dbManager *database.Manager,
	logger *logrus.Logger,
) *QueryHandler {
	if logger == nil {
		logger = logrus.New()
	}

	// Create query cache
	cacheConfig := cache.CacheConfig{
		Enabled:         cfg.QueryCache.Enabled,
		MaxSizeMB:       cfg.QueryCache.MaxSizeMB,
		TTLSeconds:      cfg.QueryCache.TTLSeconds,
		CleanupInterval: time.Duration(cfg.QueryCache.CleanupIntervalSeconds) * time.Second,
		MaxEntries:      cfg.QueryCache.MaxEntries,
	}
	queryCache := cache.NewQueryCache(cacheConfig, logger)

	return &QueryHandler{
		config:      cfg,
		queryEngine: queryEngine,
		storage:     storage,
		cache:       queryCache,
		dbManager:   dbManager,
		logger:      logger,
	}
}

// QueryRequest represents a SQL query request
type QueryRequest struct {
	SQL      string                 `json:"sql" binding:"required"`
	Database string                 `json:"database,omitempty"`
	Format   string                 `json:"format,omitempty"` // json, csv, arrow
	Options  map[string]interface{} `json:"options,omitempty"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	Columns   []query.ColumnInfo   `json:"column_info"`
	Rows      [][]interface{}      `json:"rows"`
	RowCount  int64                `json:"row_count"`
	Execution query.QueryExecution `json:"execution"`
	Metadata  query.QueryMetadata  `json:"metadata"`
	Status    string               `json:"status"`
	Message   string               `json:"message,omitempty"`
}

// Query handles SQL queries
func (h *QueryHandler) Query(c *gin.Context) {
	start := time.Now()

	// Parse request
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.WithError(err).Error("Invalid query request")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	// Get database from request or query parameter
	database := req.Database
	if database == "" {
		database = c.DefaultQuery("db", h.config.GetDatabase())
	}

	// Set default format
	if req.Format == "" {
		req.Format = "json"
	}

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"sql":         req.SQL,
		"format":      req.Format,
		"remote_addr": c.ClientIP(),
	}).Debug("Query: Processing request")

	// Validate SQL
	if err := h.validateSQL(req.SQL); err != nil {
		h.logger.WithError(err).WithField("sql", req.SQL).Warn("Invalid SQL")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid SQL query",
			"details": err.Error(),
		})
		return
	}

	// Create context with timeout
	queryTimeout := time.Duration(h.config.DuckDB.QueryTimeout) * time.Second
	ctx, cancel := context.WithTimeout(c.Request.Context(), queryTimeout)
	defer cancel()

	// Execute query
	result, err := h.queryEngine.Query(ctx, database, req.SQL)

	// Calculate duration for history
	duration := time.Since(start)

	// Save query history if database manager is available
	if h.dbManager != nil {
		go h.saveQueryHistory(database, req.SQL, result, err, duration)
	}

	if err != nil {
		// Check if error was due to timeout
		if ctx.Err() == context.DeadlineExceeded {
			h.logger.WithError(err).WithFields(logrus.Fields{
				"database": database,
				"sql":      req.SQL,
				"timeout":  queryTimeout,
			}).Warn("Query timeout exceeded")

			c.JSON(http.StatusRequestTimeout, gin.H{
				"error":           "Query timeout exceeded",
				"details":         fmt.Sprintf("Query took longer than %v seconds", h.config.DuckDB.QueryTimeout),
				"timeout_seconds": h.config.DuckDB.QueryTimeout,
			})
			return
		}

		h.logger.WithError(err).WithFields(logrus.Fields{
			"database": database,
			"sql":      req.SQL,
		}).Error("Query execution failed")

		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Query execution failed",
			"details": err.Error(),
		})
		return
	}

	// Check if result should be streamed based on row count
	if h.config.DuckDB.StreamingThreshold > 0 && result.RowCount > int64(h.config.DuckDB.StreamingThreshold) {
		h.logger.WithFields(logrus.Fields{
			"database":  database,
			"rows":      result.RowCount,
			"threshold": h.config.DuckDB.StreamingThreshold,
		}).Info("Query result exceeds streaming threshold, using streaming response")

		// Force CSV streaming for large results
		h.respondCSV(c, result)
		return
	}

	// Format response based on requested format
	switch req.Format {
	case "json":
		h.respondJSON(c, result, time.Since(start))
	case "csv":
		h.respondCSV(c, result)
	case "arrow":
		h.respondArrow(c, result)
	case "parquet":
		h.respondParquet(c, result, database, req.SQL)
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error":             "Unsupported response format",
			"supported_formats": []string{"json", "csv", "arrow", "parquet"},
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"database": database,
		"rows":     result.RowCount,
		"took":     time.Since(start),
		"format":   req.Format,
	}).Info("Query: Request completed successfully")
}

// QueryArrow handles queries returning Arrow format
func (h *QueryHandler) QueryArrow(c *gin.Context) {
	// Set format to arrow and delegate to Query handler
	c.Request.URL.RawQuery = "format=arrow"
	h.Query(c)
}

// QueryStream handles streaming queries for large result sets
func (h *QueryHandler) QueryStream(c *gin.Context) {
	start := time.Now()

	// Get query from request
	sql := c.PostForm("sql")
	if sql == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "SQL query is required",
		})
		return
	}

	// Get database
	database := c.DefaultPostForm("db", h.config.GetDatabase())

	h.logger.WithFields(logrus.Fields{
		"database":    database,
		"sql":         sql,
		"remote_addr": c.ClientIP(),
	}).Debug("QueryStream: Processing request")

	// Set response headers for streaming
	c.Header("Content-Type", "text/csv")
	c.Header("Transfer-Encoding", "chunked")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	// Create stream writer
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Streaming not supported",
		})
		return
	}

	// Execute query with streaming
	ctx := c.Request.Context()
	result, err := h.queryEngine.Query(ctx, database, sql)
	if err != nil {
		h.logger.WithError(err).Error("Stream query execution failed")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "Query execution failed",
		})
		return
	}

	// Write CSV header
	if len(result.Columns) > 0 {
		header := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			header[i] = col.Name
		}
		if _, err := fmt.Fprintf(c.Writer, "%s\n", joinStrings(header, ",")); err != nil {
			h.logger.WithError(err).Error("Failed to write CSV header")
			return
		}
		flusher.Flush()
	}

	// Stream rows
	for i, row := range result.Rows {
		rowStr := make([]string, len(row))
		for j, val := range row {
			rowStr[j] = fmt.Sprintf("%v", val)
		}

		if _, err := fmt.Fprintf(c.Writer, "%s\n", joinStrings(rowStr, ",")); err != nil {
			h.logger.WithError(err).WithField("row", i).Error("Failed to write CSV row")
			break
		}

		flusher.Flush()

		// Add small delay to prevent overwhelming the client
		if i%1000 == 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	h.logger.WithFields(logrus.Fields{
		"database": database,
		"rows":     result.RowCount,
		"took":     time.Since(start),
	}).Info("QueryStream: Request completed successfully")
}

// Measurements returns available measurements for a database
func (h *QueryHandler) Measurements(c *gin.Context) {
	database := c.DefaultQuery("db", h.config.GetDatabase())

	h.logger.WithFields(logrus.Fields{
		"database": database,
	}).Debug("Measurements: Processing request")

	ctx := c.Request.Context()
	measurements, err := h.queryEngine.GetMeasurements(ctx, database)
	if err != nil {
		h.logger.WithError(err).WithField("database", database).Error("Failed to get measurements")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to retrieve measurements",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"database":     database,
		"measurements": measurements,
		"count":        len(measurements),
	})
}

// Explain handles query explanation requests
func (h *QueryHandler) Explain(c *gin.Context) {
	start := time.Now()

	// Parse request
	var req QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid request format",
		})
		return
	}

	database := req.Database
	if database == "" {
		database = c.DefaultQuery("db", h.config.GetDatabase())
	}

	h.logger.WithFields(logrus.Fields{
		"database": database,
		"sql":      req.SQL,
	}).Debug("Explain: Processing request")

	ctx := c.Request.Context()
	plan, err := h.queryEngine.ExplainQuery(ctx, database, req.SQL)
	if err != nil {
		h.logger.WithError(err).Error("Query explanation failed")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Query explanation failed",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"database": database,
		"sql":      req.SQL,
		"plan":     plan,
		"took":     time.Since(start).String(),
	})
}

// Health handles query engine health checks
func (h *QueryHandler) Health(c *gin.Context) {
	ctx := c.Request.Context()
	if err := h.queryEngine.Health(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

// GetMetrics returns query engine metrics
func (h *QueryHandler) GetMetrics(c *gin.Context) {
	metrics := h.queryEngine.GetMetrics()
	c.JSON(http.StatusOK, metrics)
}

// validateSQL performs basic SQL validation
func (h *QueryHandler) validateSQL(sql string) error {
	// Check for empty SQL
	if sql == "" {
		return fmt.Errorf("SQL query cannot be empty")
	}

	// Basic SQL injection prevention
	sqlLower := strings.ToLower(sql)
	dangerousKeywords := []string{"drop", "delete", "truncate", "alter", "create", "insert", "update"}

	for _, keyword := range dangerousKeywords {
		if strings.Contains(sqlLower, keyword) {
			// Allow these keywords in subqueries or specific contexts
			if !h.isAllowedInContext(sqlLower, keyword) {
				return fmt.Errorf("potentially dangerous SQL keyword detected: %s", keyword)
			}
		}
	}

	return nil
}

// isAllowedInContext checks if a dangerous keyword is allowed in a specific context
func (h *QueryHandler) isAllowedInContext(sql, keyword string) bool {
	// Allow DROP in CREATE INDEX DROP, etc.
	// Allow DELETE in DELETE statements (if enabled in config)
	if keyword == "delete" && h.config.Delete.Enabled {
		return true
	}

	// This is a simplified check - in practice you'd want more sophisticated SQL parsing
	return false
}

// respondJSON sends JSON response
func (h *QueryHandler) respondJSON(c *gin.Context, result *query.QueryResult, took time.Duration) {
	response := QueryResponse{
		Columns:   result.Columns,
		Rows:      result.Rows,
		RowCount:  result.RowCount,
		Execution: result.Execution,
		Metadata:  result.Metadata,
		Status:    "success",
	}

	c.JSON(http.StatusOK, response)
}

// respondCSV sends CSV response
func (h *QueryHandler) respondCSV(c *gin.Context, result *query.QueryResult) {
	c.Header("Content-Type", "text/csv")

	// Write CSV header
	if len(result.Columns) > 0 {
		header := make([]string, len(result.Columns))
		for i, col := range result.Columns {
			header[i] = col.Name
		}
		if _, err := fmt.Fprintf(c.Writer, "%s\n", joinStrings(header, ",")); err != nil {
			h.logger.WithError(err).Error("Failed to write CSV header")
			return
		}
	}

	// Write rows
	for _, row := range result.Rows {
		rowStr := make([]string, len(row))
		for i, val := range row {
			rowStr[i] = fmt.Sprintf("%v", val)
		}
		if _, err := fmt.Fprintf(c.Writer, "%s\n", joinStrings(rowStr, ",")); err != nil {
			h.logger.WithError(err).Error("Failed to write CSV row")
			return
		}
	}
}

// respondArrow sends Arrow format response
func (h *QueryHandler) respondArrow(c *gin.Context, result *query.QueryResult) {
	// This would serialize to Apache Arrow format
	// For now, fall back to JSON
	h.respondJSON(c, result, 0)
}

// respondParquet sends Parquet format response using DuckDB's native COPY TO
func (h *QueryHandler) respondParquet(c *gin.Context, result *query.QueryResult, database, sql string) {
	// Create temporary file for Parquet export
	tempFile := fmt.Sprintf("%s/query_%d.parquet", h.config.DuckDB.TempDirectory, time.Now().UnixNano())

	h.logger.WithFields(logrus.Fields{
		"temp_file": tempFile,
		"rows":      result.RowCount,
	}).Debug("Exporting query results to Parquet")

	// Use DuckDB to export results directly to Parquet
	// We'll execute: COPY (original_query) TO 'file.parquet' (FORMAT PARQUET)
	exportSQL := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", sql, tempFile)

	// Execute export
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := h.queryEngine.Query(ctx, database, exportSQL)
	if err != nil {
		h.logger.WithError(err).Error("Failed to export Parquet")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to export to Parquet format",
			"details": err.Error(),
		})
		return
	}

	// Open the file for streaming
	file, err := os.Open(tempFile)
	if err != nil {
		h.logger.WithError(err).Error("Failed to open Parquet file")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to read Parquet file",
			"details": err.Error(),
		})
		return
	}
	defer file.Close()
	defer os.Remove(tempFile) // Clean up temp file

	// Get file info for content length
	fileInfo, err := file.Stat()
	if err != nil {
		h.logger.WithError(err).Error("Failed to stat Parquet file")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to get Parquet file info",
			"details": err.Error(),
		})
		return
	}

	// Set response headers
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename=query_result.parquet")
	c.Header("Content-Length", fmt.Sprintf("%d", fileInfo.Size()))
	c.Header("X-Row-Count", fmt.Sprintf("%d", result.RowCount))

	// Stream file to client
	if _, err := io.Copy(c.Writer, file); err != nil {
		h.logger.WithError(err).Error("Failed to stream Parquet file")
		return
	}

	h.logger.WithFields(logrus.Fields{
		"file_size": fileInfo.Size(),
		"rows":      result.RowCount,
	}).Info("Parquet export completed successfully")
}

// joinStrings joins a slice of strings with a separator
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	var b strings.Builder
	b.WriteString(strs[0])
	for _, s := range strs[1:] {
		b.WriteString(sep)
		b.WriteString(s)
	}
	return b.String()
}

// EstimateQuery estimates query execution (row count and warnings)
func (h *QueryHandler) EstimateQuery(c *gin.Context) {
	var req struct {
		SQL      string `json:"sql" binding:"required"`
		Database string `json:"database"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate SQL
	if err := h.validateSQL(req.SQL); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create COUNT(*) version of the query
	countSQL := fmt.Sprintf("SELECT COUNT(*) as estimated_rows FROM (%s) AS t", req.SQL)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute count query
	start := time.Now()
	result, err := h.queryEngine.Query(ctx, req.Database, countSQL)
	estimateTime := time.Since(start)

	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success":        false,
			"error":          fmt.Sprintf("Cannot estimate query: %v", err),
			"estimated_rows": nil,
			"warning_level":  "error",
		})
		return
	}

	// Extract row count
	var estimatedRows int64
	if len(result.Rows) > 0 && len(result.Rows[0]) > 0 {
		switch v := result.Rows[0][0].(type) {
		case int64:
			estimatedRows = v
		case int:
			estimatedRows = int64(v)
		case float64:
			estimatedRows = int64(v)
		}
	}

	// Determine warning level
	warningLevel := "none"
	var warningMessage string

	if estimatedRows > 1000000 {
		warningLevel = "high"
		warningMessage = fmt.Sprintf("⚠️ Large query: %s rows. This may take several minutes and use significant memory.", formatNumber(estimatedRows))
	} else if estimatedRows > 100000 {
		warningLevel = "medium"
		warningMessage = fmt.Sprintf("⚠️ Medium query: %s rows. This may take 30-60 seconds.", formatNumber(estimatedRows))
	} else if estimatedRows > 10000 {
		warningLevel = "low"
		warningMessage = fmt.Sprintf("📊 %s rows. Should complete quickly.", formatNumber(estimatedRows))
	} else {
		warningMessage = fmt.Sprintf("✅ Small query: %s rows.", formatNumber(estimatedRows))
	}

	h.logger.WithFields(logrus.Fields{
		"estimated_rows": estimatedRows,
		"warning_level":  warningLevel,
		"estimate_time":  estimateTime.Milliseconds(),
	}).Debug("Query estimation completed")

	c.JSON(http.StatusOK, gin.H{
		"success":           true,
		"estimated_rows":    estimatedRows,
		"warning_level":     warningLevel,
		"warning_message":   warningMessage,
		"execution_time_ms": estimateTime.Milliseconds(),
	})
}

// Close gracefully shuts down the query handler
func (h *QueryHandler) Close() error {
	h.logger.Info("QueryHandler: Shutting down")
	return h.queryEngine.Close()
}

// GetCache returns the query cache instance
func (h *QueryHandler) GetCache() *cache.QueryCache {
	return h.cache
}

// formatNumber formats a number with thousands separators
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%d,%03d", n/1000, n%1000)
	}
	millions := n / 1000000
	thousands := (n % 1000000) / 1000
	ones := n % 1000
	return fmt.Sprintf("%d,%03d,%03d", millions, thousands, ones)
}

// saveQueryHistory saves a query execution to the history database
func (h *QueryHandler) saveQueryHistory(dbName, sql string, result *query.QueryResult, err error, duration time.Duration) {
	if h.dbManager == nil {
		return
	}

	var databaseName *string
	if dbName != "" {
		databaseName = &dbName
	}

	history := &database.QueryHistory{
		SQL:          sql,
		DatabaseName: databaseName,
		DurationMs:   duration.Milliseconds(),
	}

	if err != nil {
		history.Status = "error"
		errMsg := err.Error()
		history.ErrorMessage = &errMsg
		history.RowCount = 0
	} else {
		history.Status = "success"
		if result != nil {
			history.RowCount = result.RowCount
		}
	}

	if _, saveErr := h.dbManager.AddQueryHistory(history); saveErr != nil {
		h.logger.WithError(saveErr).Warn("Failed to save query history")
	}
}

// GetQueryHistory returns recent query history
func (h *QueryHandler) GetQueryHistory(c *gin.Context) {
	if h.dbManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Query history not available",
		})
		return
	}

	// Get limit from query parameter (default: 50, max: 200)
	limit := 50
	if limitParam := c.Query("limit"); limitParam != "" {
		if parsedLimit, err := fmt.Sscanf(limitParam, "%d", &limit); err == nil && parsedLimit == 1 {
			if limit > 200 {
				limit = 200
			} else if limit < 1 {
				limit = 50
			}
		}
	}

	histories, err := h.dbManager.GetQueryHistory(limit)
	if err != nil {
		h.logger.WithError(err).Error("Failed to retrieve query history")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to retrieve query history",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"history": histories,
		"count":   len(histories),
	})
}

// DeleteQueryHistory deletes a specific query history record
func (h *QueryHandler) DeleteQueryHistory(c *gin.Context) {
	if h.dbManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Query history not available",
		})
		return
	}

	var id int64
	if _, err := fmt.Sscanf(c.Param("id"), "%d", &id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid history ID",
		})
		return
	}

	if err := h.dbManager.DeleteQueryHistory(id); err != nil {
		h.logger.WithError(err).Error("Failed to delete query history")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to delete query history",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Query history deleted successfully",
	})
}

// ClearQueryHistory deletes all query history records
func (h *QueryHandler) ClearQueryHistory(c *gin.Context) {
	if h.dbManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "Query history not available",
		})
		return
	}

	if err := h.dbManager.ClearQueryHistory(); err != nil {
		h.logger.WithError(err).Error("Failed to clear query history")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to clear query history",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Query history cleared successfully",
	})
}
