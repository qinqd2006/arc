package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/arc-core/arc-go/internal/storage"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/sirupsen/logrus"
)

// DuckDBConfig holds configuration for DuckDB query engine
type DuckDBConfig struct {
	PoolSize          int    `toml:"pool_size"`
	MaxQueueSize      int    `toml:"max_queue_size"`
	MemoryLimit       string `toml:"memory_limit"`
	Threads           int    `toml:"threads"`
	TempDirectory     string `toml:"temp_directory"`
	EnableObjectCache bool   `toml:"enable_object_cache"`
}

// DuckDBEngine manages DuckDB connections and query execution
type DuckDBEngine struct {
	config      DuckDBConfig
	storage     storage.Backend
	pathBuilder storage.PathBuilder
	logger      *logrus.Logger
	connPool    chan *sql.DB
	mu          sync.RWMutex
	connections []*sql.DB
	isClosed    bool

	// Metrics
	metrics DuckDBMetrics
}

// DuckDBMetrics holds query engine performance metrics
type DuckDBMetrics struct {
	QueriesExecuted    int64         `json:"queries_executed"`
	QueryErrors        int64         `json:"query_errors"`
	ActiveConnections  int           `json:"active_connections"`
	TotalConnections   int           `json:"total_connections"`
	AverageQueryTime   time.Duration `json:"average_query_time"`
	LastQuery          time.Time     `json:"last_query"`
	CacheHits          int64         `json:"cache_hits"`
	CacheMisses        int64         `json:"cache_misses"`
	ParquetFilesLoaded int64         `json:"parquet_files_loaded"`
	BytesScanned       int64         `json:"bytes_scanned"`
}

// QueryResult represents a query result
type QueryResult struct {
	Columns   []ColumnInfo    `json:"column_info"`
	Rows      [][]interface{} `json:"rows"`
	RowCount  int64           `json:"row_count"`
	Execution QueryExecution  `json:"execution"`
	Metadata  QueryMetadata   `json:"metadata"`
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// QueryExecution represents query execution information
type QueryExecution struct {
	Query    string        `json:"query"`
	Duration time.Duration `json:"duration"`
	Rows     int64         `json:"rows"`
	Bytes    int64         `json:"bytes_scanned"`
}

// QueryMetadata represents additional query metadata
type QueryMetadata struct {
	Database  string    `json:"database"`
	Tables    []string  `json:"tables"`
	Timestamp time.Time `json:"timestamp"`
	CacheHit  bool      `json:"cache_hit"`
}

// NewDuckDBEngine creates a new DuckDB query engine
func NewDuckDBEngine(
	config DuckDBConfig,
	storage storage.Backend,
	pathBuilder storage.PathBuilder,
	logger *logrus.Logger,
) (*DuckDBEngine, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// Set defaults
	if config.PoolSize <= 0 {
		config.PoolSize = 5
	}
	if config.MaxQueueSize <= 0 {
		config.MaxQueueSize = 100
	}
	if config.MemoryLimit == "" {
		config.MemoryLimit = "8GB"
	}
	if config.Threads <= 0 {
		config.Threads = 4
	}
	if config.TempDirectory == "" {
		config.TempDirectory = "./data/duckdb_tmp"
	}

	engine := &DuckDBEngine{
		config:      config,
		storage:     storage,
		pathBuilder: pathBuilder,
		logger:      logger,
		connPool:    make(chan *sql.DB, config.PoolSize),
		connections: make([]*sql.DB, 0, config.PoolSize),
		metrics:     DuckDBMetrics{},
	}

	// Initialize connection pool
	if err := engine.initializePool(); err != nil {
		return nil, fmt.Errorf("failed to initialize connection pool: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"pool_size":      config.PoolSize,
		"memory_limit":   config.MemoryLimit,
		"threads":        config.Threads,
		"temp_directory": config.TempDirectory,
	}).Info("DuckDB engine initialized")

	return engine, nil
}

// initializePool creates the initial connection pool
func (e *DuckDBEngine) initializePool() error {
	for i := 0; i < e.config.PoolSize; i++ {
		conn, err := e.createConnection()
		if err != nil {
			return fmt.Errorf("failed to create connection %d: %w", i, err)
		}

		e.connections = append(e.connections, conn)
		e.connPool <- conn
		e.metrics.TotalConnections++
	}

	return nil
}

// createConnection creates a new DuckDB connection
func (e *DuckDBEngine) createConnection() (*sql.DB, error) {
	// Create in-memory DuckDB database
	conn, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB connection: %w", err)
	}

	// Configure connection
	configQueries := []string{
		fmt.Sprintf("SET memory_limit='%s'", e.config.MemoryLimit),
		fmt.Sprintf("SET threads=%d", e.config.Threads),
		fmt.Sprintf("SET temp_directory='%s'", e.config.TempDirectory),
	}

	if e.config.EnableObjectCache {
		configQueries = append(configQueries, "SET enable_object_cache=true")
	}

	for _, query := range configQueries {
		if _, err := conn.Exec(query); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to execute config query '%s': %w", query, err)
		}
	}

	// Install and load parquet extension
	if _, err := conn.Exec("INSTALL parquet"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to install parquet extension: %w", err)
	}
	if _, err := conn.Exec("LOAD parquet"); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to load parquet extension: %w", err)
	}

	return conn, nil
}

// getConnection gets a connection from the pool
func (e *DuckDBEngine) getConnection(ctx context.Context) (*sql.DB, error) {
	select {
	case conn := <-e.connPool:
		e.mu.Lock()
		e.metrics.ActiveConnections++
		e.mu.Unlock()
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timeout waiting for connection")
	}
}

// releaseConnection returns a connection to the pool
func (e *DuckDBEngine) releaseConnection(conn *sql.DB) {
	e.mu.Lock()
	e.metrics.ActiveConnections--
	e.mu.Unlock()

	select {
	case e.connPool <- conn:
		// Connection returned to pool
	default:
		// Pool is full, close connection
		conn.Close()
		e.mu.Lock()
		e.metrics.TotalConnections--
		e.mu.Unlock()
	}
}

// Query executes a SQL query and returns results
func (e *DuckDBEngine) Query(ctx context.Context, database, query string) (*QueryResult, error) {
	start := time.Now()
	defer func() {
		e.metrics.QueriesExecuted++
		e.metrics.LastQuery = start

		// Update average query time
		if e.metrics.QueriesExecuted > 0 {
			e.metrics.AverageQueryTime = time.Duration(
				int64(e.metrics.AverageQueryTime)*e.metrics.QueriesExecuted/int64(e.metrics.QueriesExecuted+1) +
					int64(time.Since(start))/int64(e.metrics.QueriesExecuted+1),
			)
		}
	}()

	// Get connection from pool
	conn, err := e.getConnection(ctx)
	if err != nil {
		e.metrics.QueryErrors++
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer e.releaseConnection(conn)

	// Setup storage for the database if needed
	if err := e.setupStorage(conn, database); err != nil {
		e.metrics.QueryErrors++
		return nil, fmt.Errorf("failed to setup storage: %w", err)
	}

	e.logger.WithFields(logrus.Fields{
		"database": database,
		"query":    query,
	}).Debug("DuckDB: Executing query")

	// Execute query
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		e.metrics.QueryErrors++
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Read column information
	columns, err := rows.Columns()
	if err != nil {
		e.metrics.QueryErrors++
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Read column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		e.metrics.QueryErrors++
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Prepare result
	result := &QueryResult{
		Rows:    make([][]interface{}, 0),
		Columns: make([]ColumnInfo, len(columns)),
		Execution: QueryExecution{
			Query:    query,
			Duration: time.Since(start),
		},
		Metadata: QueryMetadata{
			Database:  database,
			Timestamp: start,
		},
	}

	// Build column info
	for i, col := range columns {
		result.Columns[i] = ColumnInfo{
			Name:     col,
			Type:     columnTypes[i].DatabaseTypeName(),
			Nullable: true, // DuckDB columns are generally nullable
		}
	}

	// Read rows
	for rows.Next() {
		// Create slice for row values
		row := make([]interface{}, len(columns))
		rowPtrs := make([]interface{}, len(columns))
		for i := range row {
			rowPtrs[i] = &row[i]
		}

		// Scan row
		if err := rows.Scan(rowPtrs...); err != nil {
			e.metrics.QueryErrors++
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result.Rows = append(result.Rows, row)
	}

	// Check for row scan errors
	if err := rows.Err(); err != nil {
		e.metrics.QueryErrors++
		return nil, fmt.Errorf("row scan error: %w", err)
	}

	result.RowCount = int64(len(result.Rows))
	result.Execution.Rows = result.RowCount

	e.logger.WithFields(logrus.Fields{
		"database": database,
		"rows":     result.RowCount,
		"columns":  len(columns),
		"duration": time.Since(start),
		"query":    query,
	}).Debug("DuckDB: Query completed successfully")

	return result, nil
}

// setupStorage sets up DuckDB to read from storage backend
func (e *DuckDBEngine) setupStorage(conn *sql.DB, database string) error {
	// Get all measurements for the database
	prefix := e.pathBuilder.BuildDatabasePrefix(database)
	objects, err := e.storage.List(context.Background(), prefix)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	e.logger.WithFields(logrus.Fields{
		"database":     database,
		"prefix":       prefix,
		"object_count": len(objects),
	}).Debug("DuckDB: Listing storage objects")

	// Group objects by measurement
	measurements := make(map[string][]string)
	for _, obj := range objects {
		if e.pathBuilder.IsSystemPath(obj.Key) {
			continue
		}

		_, measurement, _, _, _, _, err := e.pathBuilder.ParsePath(obj.Key)
		if err != nil {
			e.logger.WithError(err).WithField("key", obj.Key).Debug("Failed to parse path")
			continue
		}

		// Build full file path based on storage type
		var fullPath string
		if e.storage.Type() == "local" {
			// For local storage, use the base path from config
			fullPath = fmt.Sprintf("./data/arc/%s", obj.Key)
		} else {
			// For remote storage (S3, MinIO, GCS), use the key directly
			fullPath = obj.Key
		}

		measurements[measurement] = append(measurements[measurement], fullPath)
	}

	e.logger.WithFields(logrus.Fields{
		"database":          database,
		"measurement_count": len(measurements),
	}).Debug("DuckDB: Extracted measurements")

	// Create views for each measurement
	viewsCreated := 0
	for measurement, files := range measurements {
		if len(files) == 0 {
			continue
		}

		// Create view with just measurement name (InfluxDB-style)
		viewName := measurement

		// Escape single quotes in file paths and create file list
		escapedFiles := make([]string, len(files))
		for i, f := range files {
			escapedFiles[i] = strings.ReplaceAll(f, "'", "''")
		}
		fileList := fmt.Sprintf("'%s'", strings.Join(escapedFiles, "','"))

		// Try to create view with multiple files, or single file if only one
		var createViewSQL string
		if len(files) == 1 {
			createViewSQL = fmt.Sprintf(
				"CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
				measurement, escapedFiles[0],
			)
		} else {
			createViewSQL = fmt.Sprintf(
				"CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet([%s])",
				measurement, fileList,
			)
		}

		if _, err := conn.Exec(createViewSQL); err != nil {
			e.logger.WithError(err).WithFields(logrus.Fields{
				"view_name":  viewName,
				"files":      files,
				"file_count": len(files),
				"database":   database,
				"sql":        createViewSQL,
			}).Warn("Failed to create view for measurement")
		} else {
			viewsCreated++
			e.logger.WithFields(logrus.Fields{
				"view_name":  viewName,
				"file_count": len(files),
				"database":   database,
			}).Debug("Created view for measurement")
		}
	}

	e.logger.WithFields(logrus.Fields{
		"database":           database,
		"views_created":      viewsCreated,
		"total_measurements": len(measurements),
	}).Debug("DuckDB: Storage setup complete")

	if viewsCreated == 0 && len(measurements) > 0 {
		e.logger.WithFields(logrus.Fields{
			"database":          database,
			"measurement_count": len(measurements),
		}).Warn("No views were created despite having measurements - check parquet files")
	}

	return nil
}

// GetMeasurements returns all available measurements for a database
func (e *DuckDBEngine) GetMeasurements(ctx context.Context, database string) ([]string, error) {
	// List objects in storage to get measurements
	prefix := e.pathBuilder.BuildDatabasePrefix(database)
	objects, err := e.storage.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Extract unique measurement names
	measurementSet := make(map[string]bool)
	for _, obj := range objects {
		if e.pathBuilder.IsSystemPath(obj.Key) {
			continue
		}

		_, measurement, _, _, _, _, err := e.pathBuilder.ParsePath(obj.Key)
		if err != nil {
			continue
		}

		measurementSet[measurement] = true
	}

	// Convert to sorted slice
	measurements := make([]string, 0, len(measurementSet))
	for measurement := range measurementSet {
		measurements = append(measurements, measurement)
	}

	return measurements, nil
}

// ExplainQuery returns the query execution plan
func (e *DuckDBEngine) ExplainQuery(ctx context.Context, database, query string) (string, error) {
	explainQuery := fmt.Sprintf("EXPLAIN ANALYZE %s", query)

	result, err := e.Query(ctx, database, explainQuery)
	if err != nil {
		return "", fmt.Errorf("failed to explain query: %w", err)
	}

	// Concatenate all explanation lines
	var explanation strings.Builder
	for _, row := range result.Rows {
		if line, ok := row[0].(string); ok {
			explanation.WriteString(line)
			explanation.WriteString("\n")
		}
	}

	return explanation.String(), nil
}

// GetMetrics returns current query engine metrics
func (e *DuckDBEngine) GetMetrics() DuckDBMetrics {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.metrics
}

// ResetMetrics resets the query engine metrics
func (e *DuckDBEngine) ResetMetrics() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics = DuckDBMetrics{
		TotalConnections: e.metrics.TotalConnections,
	}
}

// Close closes the query engine and all connections
func (e *DuckDBEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.isClosed {
		return nil
	}

	e.logger.Info("DuckDB engine: Closing connections")

	// Close all connections
	for _, conn := range e.connections {
		if err := conn.Close(); err != nil {
			e.logger.WithError(err).Warn("Failed to close connection")
		}
	}

	// Clear connection pool
	close(e.connPool)
	e.connPool = make(chan *sql.DB, e.config.PoolSize)
	e.connections = e.connections[:0]
	e.isClosed = true

	e.logger.Info("DuckDB engine: Closed")
	return nil
}

// Health checks the health of the query engine
func (e *DuckDBEngine) Health(ctx context.Context) error {
	if e.isClosed {
		return fmt.Errorf("query engine is closed")
	}

	// Try to get a connection and execute a simple query
	conn, err := e.getConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer e.releaseConnection(conn)

	var result int
	if err := conn.QueryRowContext(ctx, "SELECT 1").Scan(&result); err != nil {
		return fmt.Errorf("health check query failed: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected health check result: %d", result)
	}

	return nil
}
