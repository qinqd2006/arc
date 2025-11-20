package exporter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/arc-core/arc-go/internal/query"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/sirupsen/logrus"
)

// Influx1xExporter exports data to InfluxDB 1.x
type Influx1xExporter struct {
	config Influx1xConfig
	logger *logrus.Logger
	client client.Client
}

// Influx1xConfig holds InfluxDB 1.x configuration
type Influx1xConfig struct {
	Host            string        `json:"host"`
	Port            int           `json:"port"`
	Database        string        `json:"database"`
	Username        string        `json:"username,omitempty"`
	Password        string        `json:"password,omitempty"`
	RetentionPolicy string        `json:"retention_policy,omitempty"`
	BatchSize       int           `json:"batch_size"`
	FlushInterval   time.Duration `json:"flush_interval"`
	MaxRetries      int           `json:"max_retries"`
	RetryDelay      time.Duration `json:"retry_delay"`
	Timeout         time.Duration `json:"timeout"`
}

// NewInflux1xExporter creates a new InfluxDB 1.x exporter
func NewInflux1xExporter(config Influx1xConfig, logger *logrus.Logger) *Influx1xExporter {
	if logger == nil {
		logger = logrus.New()
	}

	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	return &Influx1xExporter{
		config: config,
		logger: logger,
	}
}

// Export exports data to InfluxDB 1.x
func (e *Influx1xExporter) Export(ctx context.Context, req *ExportRequest, queryEngine *query.DuckDBEngine) (*ExportResult, error) {
	// Connect to InfluxDB
	if err := e.connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to InfluxDB: %w", err)
	}
	defer e.client.Close()

	result := &ExportResult{}

	// Build SQL query for data export
	sqlQuery := e.buildExportQuery(req)

	e.logger.WithFields(logrus.Fields{
		"database":     req.Database,
		"measurements": req.Measurements,
		"start_time":   req.StartTime,
		"end_time":     req.EndTime,
		"sql":          sqlQuery,
	}).Info("Executing export query")

	// Execute query with pagination
	recordsCount, bytesWritten, err := e.exportWithPagination(ctx, sqlQuery, req, queryEngine, result)
	if err != nil {
		return nil, fmt.Errorf("export failed: %w", err)
	}

	result.RecordsCount = recordsCount
	result.BytesWritten = bytesWritten

	e.logger.WithFields(logrus.Fields{
		"records_count": recordsCount,
		"bytes_written": bytesWritten,
	}).Info("Export to InfluxDB 1.x completed")

	return result, nil
}

// connect establishes connection to InfluxDB
func (e *Influx1xExporter) connect() error {
	// Build connection URL
	host := e.config.Host
	if !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		host = "http://" + host
	}

	url := fmt.Sprintf("%s:%d", host, e.config.Port)

	// Create client
	var err error
	e.client, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     url,
		Username: e.config.Username,
		Password: e.config.Password,
	})

	if err != nil {
		return fmt.Errorf("failed to create InfluxDB client: %w", err)
	}

	// Test connection
	_, _, err = e.client.Ping(e.config.Timeout)
	if err != nil {
		return fmt.Errorf("failed to ping InfluxDB: %w", err)
	}

	e.logger.WithFields(logrus.Fields{
		"url":      url,
		"database": e.config.Database,
	}).Info("Connected to InfluxDB 1.x")

	return nil
}

// buildExportQuery builds SQL query for data export
func (e *Influx1xExporter) buildExportQuery(req *ExportRequest) string {
	// Build SELECT clause
	selectFields := "*"
	if len(req.Fields) > 0 {
		selectFields = strings.Join(req.Fields, ", ")
	}

	// Build FROM clause
	fromClause := ""
	if len(req.Measurements) > 0 {
		var tables []string
		for _, measurement := range req.Measurements {
			tables = append(tables, fmt.Sprintf("%s_%s", req.Database, measurement))
		}
		fromClause = fmt.Sprintf("FROM %s", strings.Join(tables, ", "))
	} else {
		fromClause = fmt.Sprintf("FROM %s_*", req.Database)
	}

	// Build WHERE clause
	whereClause := fmt.Sprintf("WHERE time >= '%s' AND time <= '%s'",
		req.StartTime.Format("2006-01-02 15:04:05.000000000"),
		req.EndTime.Format("2006-01-02 15:04:05.000000000"))

	// Add filters
	if len(req.Filters) > 0 {
		for key, value := range req.Filters {
			switch v := value.(type) {
			case string:
				whereClause += fmt.Sprintf(" AND %s = '%s'", key, v)
			case int, int64, float64:
				whereClause += fmt.Sprintf(" AND %s = %v", key, v)
			case []interface{}:
				// Handle IN clause
				var values []string
				for _, item := range v {
					if str, ok := item.(string); ok {
						values = append(values, fmt.Sprintf("'%s'", str))
					} else {
						values = append(values, fmt.Sprintf("%v", item))
					}
				}
				whereClause += fmt.Sprintf(" AND %s IN (%s)", key, strings.Join(values, ", "))
			}
		}
	}

	// Build LIMIT clause
	limitClause := ""
	if req.Limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", req.Limit)
	}

	// Combine all clauses
	query := fmt.Sprintf("SELECT %s %s %s ORDER BY time %s",
		selectFields, fromClause, whereClause, limitClause)

	return query
}

// exportWithPagination exports data using pagination to handle large datasets
func (e *Influx1xExporter) exportWithPagination(ctx context.Context, sqlQuery string, req *ExportRequest, queryEngine *query.DuckDBEngine, result *ExportResult) (int64, int64, error) {
	batchSize := int64(e.config.BatchSize)
	offset := int64(0)
	totalRecords := int64(0)
	totalBytes := int64(0)

	for {
		// Add pagination to query
		paginatedQuery := fmt.Sprintf("%s LIMIT %d OFFSET %d", sqlQuery, batchSize, offset)

		// Execute query
		queryResult, err := queryEngine.Query(ctx, req.Database, paginatedQuery)
		if err != nil {
			return totalRecords, totalBytes, fmt.Errorf("query failed: %w", err)
		}

		if len(queryResult.Rows) == 0 {
			break // No more data
		}

		// Convert to InfluxDB points
		points, err := e.convertToPoints(queryResult)
		if err != nil {
			return totalRecords, totalBytes, fmt.Errorf("failed to convert to points: %w", err)
		}

		// Write batch to InfluxDB
		batchSizeResult, err := e.writeBatch(ctx, points)
		if err != nil {
			return totalRecords, totalBytes, fmt.Errorf("failed to write batch: %w", err)
		}

		totalRecords += int64(len(points))
		totalBytes += batchSizeResult.BytesWritten

		// Update result
		if len(batchSizeResult.FilesCreated) > 0 {
			result.FilesCreated = append(result.FilesCreated, batchSizeResult.FilesCreated...)
		}

		e.logger.WithFields(logrus.Fields{
			"batch_records": len(points),
			"total_records": totalRecords,
			"offset":        offset,
		}).Debug("Export batch completed")

		// Check if we got fewer records than batch size (indicates last batch)
		if int64(len(queryResult.Rows)) < batchSize {
			break
		}

		offset += batchSize

		// Check context for cancellation
		select {
		case <-ctx.Done():
			return totalRecords, totalBytes, ctx.Err()
		default:
		}
	}

	return totalRecords, totalBytes, nil
}

// convertToPoints converts query results to InfluxDB points
func (e *Influx1xExporter) convertToPoints(queryResult *query.QueryResult) ([]*client.Point, error) {
	var points []*client.Point

	// Find measurement, time, and tag columns
	measurementCol := -1
	timeCol := -1
	tagCols := make(map[int]string)
	fieldCols := make(map[int]string)

	for i, col := range queryResult.Columns {
		colName := col.Name // Use col.Name since Columns is []ColumnInfo
		switch colName {
		case "measurement":
			measurementCol = i
		case "time":
			timeCol = i
		default:
			// Heuristic: if column name contains common tag names, treat as tag
			if isTagColumn(colName) {
				tagCols[i] = colName
			} else {
				fieldCols[i] = colName
			}
		}
	}

	// Convert each row
	for _, row := range queryResult.Rows {
		// Extract measurement name
		measurement := "unknown"
		if measurementCol >= 0 && measurementCol < len(row) {
			if m, ok := row[measurementCol].(string); ok {
				measurement = m
			}
		}

		// Extract timestamp
		var timestamp time.Time
		if timeCol >= 0 && timeCol < len(row) {
			if t, ok := row[timeCol].(time.Time); ok {
				timestamp = t
			} else if t, ok := row[timeCol].(int64); ok {
				timestamp = time.Unix(0, t)
			}
		} else {
			timestamp = time.Now()
		}

		// Create tags
		tags := make(map[string]string)
		for colIdx, colName := range tagCols {
			if colIdx < len(row) {
				if val, ok := row[colIdx].(string); ok {
					tags[colName] = val
				}
			}
		}

		// Create fields
		fields := make(map[string]interface{})
		for colIdx, colName := range fieldCols {
			if colIdx < len(row) {
				fields[colName] = row[colIdx]
			}
		}

		// Create point
		point, err := client.NewPoint(
			measurement,
			tags,
			fields,
			timestamp,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create point: %w", err)
		}

		points = append(points, point)
	}

	return points, nil
}

// isTagColumn determines if a column should be treated as a tag
func isTagColumn(columnName string) bool {
	tagColumns := []string{
		"host", "hostname", "server", "node",
		"region", "zone", "datacenter",
		"environment", "env",
		"service", "application", "app",
		"version", "build",
		"instance", "container",
	}

	lowerName := strings.ToLower(columnName)
	for _, tag := range tagColumns {
		if strings.Contains(lowerName, tag) {
			return true
		}
	}

	return false
}

// writeBatch writes a batch of points to InfluxDB
func (e *Influx1xExporter) writeBatch(ctx context.Context, points []*client.Point) (*BatchResult, error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        e.config.Database,
		RetentionPolicy: e.config.RetentionPolicy,
		Precision:       "ns",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create batch points: %w", err)
	}

	for _, point := range points {
		bp.AddPoint(point)
	}

	// Write with retry logic
	var lastErr error
	for attempt := 0; attempt <= e.config.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(e.config.RetryDelay):
			}
		}

		err := e.client.Write(bp)
		if err == nil {
			// Success
			batchSize := int64(len(points))
			estimatedBytes := batchSize * 200 // Rough estimate
			return &BatchResult{
				RecordsCount: batchSize,
				BytesWritten: estimatedBytes,
				FilesCreated: []string{}, // InfluxDB doesn't create files
			}, nil
		}

		lastErr = err
		e.logger.WithError(err).WithField("attempt", attempt+1).Warn("Failed to write batch to InfluxDB")
	}

	return nil, fmt.Errorf("failed to write batch after %d attempts: %w", e.config.MaxRetries+1, lastErr)
}

// BatchResult represents the result of a batch write operation
type BatchResult struct {
	RecordsCount int64    `json:"records_count"`
	BytesWritten int64    `json:"bytes_written"`
	FilesCreated []string `json:"files_created"`
}
