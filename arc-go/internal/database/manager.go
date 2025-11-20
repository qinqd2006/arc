package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

// Manager handles all database connections and operations
type Manager struct {
	db     *sql.DB
	dbPath string
	mu     sync.RWMutex
	logger *logrus.Logger
}

// NewManager creates a new database manager
func NewManager(dbPath string, logger *logrus.Logger) (*Manager, error) {
	if logger == nil {
		logger = logrus.New()
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	mgr := &Manager{
		db:     db,
		dbPath: dbPath,
		logger: logger,
	}

	// Initialize schema
	if err := mgr.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	logger.WithField("db_path", dbPath).Info("Database manager initialized")
	return mgr, nil
}

// initSchema creates all tables
func (m *Manager) initSchema() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Enable WAL mode for better concurrency
	if _, err := m.db.Exec(EnableWALMode); err != nil {
		return fmt.Errorf("failed to enable WAL mode: %w", err)
	}

	// Set busy timeout
	if _, err := m.db.Exec(SetBusyTimeout); err != nil {
		return fmt.Errorf("failed to set busy timeout: %w", err)
	}

	// Create all tables
	for _, stmt := range AllTables() {
		if _, err := m.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}

	m.logger.Debug("Database schema initialized successfully")
	return nil
}

// Close closes the database connection
func (m *Manager) Close() error {
	return m.db.Close()
}

// ============================================================================
// InfluxDB Connections
// ============================================================================

// GetInfluxConnections returns all InfluxDB connections
func (m *Manager) GetInfluxConnections() ([]*InfluxConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, version, host, port, database_name, username, password,
	                 token, org, bucket, database, ssl, is_active, created_at, updated_at
	          FROM influx_connections ORDER BY created_at DESC`

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []*InfluxConnection
	for rows.Next() {
		conn := &InfluxConnection{}
		err := rows.Scan(&conn.ID, &conn.Name, &conn.Version, &conn.Host, &conn.Port,
			&conn.DatabaseName, &conn.Username, &conn.Password, &conn.Token, &conn.Org,
			&conn.Bucket, &conn.Database, &conn.SSL, &conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt)
		if err != nil {
			return nil, err
		}
		connections = append(connections, conn)
	}

	return connections, rows.Err()
}

// AddInfluxConnection adds a new InfluxDB connection
func (m *Manager) AddInfluxConnection(conn *InfluxConnection) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO influx_connections
	          (name, version, host, port, database_name, username, password, token, org, bucket, database, ssl, is_active)
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, conn.Name, conn.Version, conn.Host, conn.Port,
		conn.DatabaseName, conn.Username, conn.Password, conn.Token, conn.Org,
		conn.Bucket, conn.Database, conn.SSL, conn.IsActive)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// UpdateInfluxConnection updates an existing InfluxDB connection
func (m *Manager) UpdateInfluxConnection(id int64, conn *InfluxConnection) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `UPDATE influx_connections
	          SET name=?, version=?, host=?, port=?, database_name=?, username=?, password=?,
	              token=?, org=?, bucket=?, database=?, ssl=?, updated_at=CURRENT_TIMESTAMP
	          WHERE id=?`

	_, err := m.db.Exec(query, conn.Name, conn.Version, conn.Host, conn.Port,
		conn.DatabaseName, conn.Username, conn.Password, conn.Token, conn.Org,
		conn.Bucket, conn.Database, conn.SSL, id)

	return err
}

// DeleteInfluxConnection deletes an InfluxDB connection
func (m *Manager) DeleteInfluxConnection(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM influx_connections WHERE id=?", id)
	return err
}

// GetActiveInfluxConnection returns the active InfluxDB connection
func (m *Manager) GetActiveInfluxConnection() (*InfluxConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, version, host, port, database_name, username, password,
	                 token, org, bucket, database, ssl, is_active, created_at, updated_at
	          FROM influx_connections WHERE is_active=1 LIMIT 1`

	conn := &InfluxConnection{}
	err := m.db.QueryRow(query).Scan(&conn.ID, &conn.Name, &conn.Version, &conn.Host, &conn.Port,
		&conn.DatabaseName, &conn.Username, &conn.Password, &conn.Token, &conn.Org,
		&conn.Bucket, &conn.Database, &conn.SSL, &conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// SetActiveInfluxConnection sets the active InfluxDB connection
func (m *Manager) SetActiveInfluxConnection(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Deactivate all connections
	if _, err := tx.Exec("UPDATE influx_connections SET is_active=0"); err != nil {
		return err
	}

	// Activate the specified connection
	if _, err := tx.Exec("UPDATE influx_connections SET is_active=1 WHERE id=?", id); err != nil {
		return err
	}

	return tx.Commit()
}

// ============================================================================
// Storage Connections
// ============================================================================

// GetStorageConnections returns all storage connections
func (m *Manager) GetStorageConnections() ([]*StorageConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, backend, endpoint, access_key, secret_key, bucket, base_path,
	                 database, region, ssl, use_directory_bucket, availability_zone, project_id,
	                 credentials_json, credentials_file, hmac_key_id, hmac_secret, is_active,
	                 created_at, updated_at
	          FROM storage_connections ORDER BY created_at DESC`

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []*StorageConnection
	for rows.Next() {
		conn := &StorageConnection{}
		err := rows.Scan(&conn.ID, &conn.Name, &conn.Backend, &conn.Endpoint, &conn.AccessKey,
			&conn.SecretKey, &conn.Bucket, &conn.BasePath, &conn.Database, &conn.Region,
			&conn.SSL, &conn.UseDirectoryBucket, &conn.AvailabilityZone, &conn.ProjectID,
			&conn.CredentialsJSON, &conn.CredentialsFile, &conn.HMACKeyID, &conn.HMACSecret,
			&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt)
		if err != nil {
			return nil, err
		}
		connections = append(connections, conn)
	}

	return connections, rows.Err()
}

// AddStorageConnection adds a new storage connection
func (m *Manager) AddStorageConnection(conn *StorageConnection) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO storage_connections
	          (name, backend, endpoint, access_key, secret_key, bucket, base_path, database,
	           region, ssl, use_directory_bucket, availability_zone, project_id, credentials_json,
	           credentials_file, hmac_key_id, hmac_secret, is_active)
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, conn.Name, conn.Backend, conn.Endpoint, conn.AccessKey,
		conn.SecretKey, conn.Bucket, conn.BasePath, conn.Database, conn.Region, conn.SSL,
		conn.UseDirectoryBucket, conn.AvailabilityZone, conn.ProjectID, conn.CredentialsJSON,
		conn.CredentialsFile, conn.HMACKeyID, conn.HMACSecret, conn.IsActive)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// UpdateStorageConnection updates an existing storage connection
func (m *Manager) UpdateStorageConnection(id int64, conn *StorageConnection) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `UPDATE storage_connections
	          SET name=?, backend=?, endpoint=?, access_key=?, secret_key=?, bucket=?,
	              base_path=?, database=?, region=?, ssl=?, use_directory_bucket=?,
	              availability_zone=?, project_id=?, credentials_json=?, credentials_file=?,
	              hmac_key_id=?, hmac_secret=?, updated_at=CURRENT_TIMESTAMP
	          WHERE id=?`

	_, err := m.db.Exec(query, conn.Name, conn.Backend, conn.Endpoint, conn.AccessKey,
		conn.SecretKey, conn.Bucket, conn.BasePath, conn.Database, conn.Region, conn.SSL,
		conn.UseDirectoryBucket, conn.AvailabilityZone, conn.ProjectID, conn.CredentialsJSON,
		conn.CredentialsFile, conn.HMACKeyID, conn.HMACSecret, id)

	return err
}

// DeleteStorageConnection deletes a storage connection
func (m *Manager) DeleteStorageConnection(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM storage_connections WHERE id=?", id)
	return err
}

// GetActiveStorageConnection returns the active storage connection
func (m *Manager) GetActiveStorageConnection() (*StorageConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, backend, endpoint, access_key, secret_key, bucket, base_path,
	                 database, region, ssl, use_directory_bucket, availability_zone, project_id,
	                 credentials_json, credentials_file, hmac_key_id, hmac_secret, is_active,
	                 created_at, updated_at
	          FROM storage_connections WHERE is_active=1 LIMIT 1`

	conn := &StorageConnection{}
	err := m.db.QueryRow(query).Scan(&conn.ID, &conn.Name, &conn.Backend, &conn.Endpoint,
		&conn.AccessKey, &conn.SecretKey, &conn.Bucket, &conn.BasePath, &conn.Database,
		&conn.Region, &conn.SSL, &conn.UseDirectoryBucket, &conn.AvailabilityZone,
		&conn.ProjectID, &conn.CredentialsJSON, &conn.CredentialsFile, &conn.HMACKeyID,
		&conn.HMACSecret, &conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// SetActiveStorageConnection sets the active storage connection
func (m *Manager) SetActiveStorageConnection(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Deactivate all connections
	if _, err := tx.Exec("UPDATE storage_connections SET is_active=0"); err != nil {
		return err
	}

	// Activate the specified connection
	if _, err := tx.Exec("UPDATE storage_connections SET is_active=1 WHERE id=?", id); err != nil {
		return err
	}

	return tx.Commit()
}

// ============================================================================
// HTTP JSON Connections
// ============================================================================

// GetHTTPJSONConnections returns all HTTP JSON connections
func (m *Manager) GetHTTPJSONConnections() ([]*HTTPJSONConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, endpoint_url, timestamp_field, auth_type, auth_config,
	                 http_method, headers, query_params, request_body, data_path,
	                 field_mappings, pagination_type, pagination_config, is_active,
	                 created_at, updated_at
	          FROM http_json_connections ORDER BY created_at DESC`

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []*HTTPJSONConnection
	for rows.Next() {
		conn := &HTTPJSONConnection{}
		err := rows.Scan(&conn.ID, &conn.Name, &conn.EndpointURL, &conn.TimestampField,
			&conn.AuthType, &conn.AuthConfig, &conn.HTTPMethod, &conn.Headers,
			&conn.QueryParams, &conn.RequestBody, &conn.DataPath, &conn.FieldMappings,
			&conn.PaginationType, &conn.PaginationConfig, &conn.IsActive,
			&conn.CreatedAt, &conn.UpdatedAt)
		if err != nil {
			return nil, err
		}
		connections = append(connections, conn)
	}

	return connections, rows.Err()
}

// AddHTTPJSONConnection adds a new HTTP JSON connection
func (m *Manager) AddHTTPJSONConnection(conn *HTTPJSONConnection) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO http_json_connections
	          (name, endpoint_url, timestamp_field, auth_type, auth_config, http_method,
	           headers, query_params, request_body, data_path, field_mappings, pagination_type,
	           pagination_config, is_active)
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, conn.Name, conn.EndpointURL, conn.TimestampField,
		conn.AuthType, conn.AuthConfig, conn.HTTPMethod, conn.Headers, conn.QueryParams,
		conn.RequestBody, conn.DataPath, conn.FieldMappings, conn.PaginationType,
		conn.PaginationConfig, conn.IsActive)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// UpdateHTTPJSONConnection updates an existing HTTP JSON connection
func (m *Manager) UpdateHTTPJSONConnection(id int64, conn *HTTPJSONConnection) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `UPDATE http_json_connections
	          SET name=?, endpoint_url=?, timestamp_field=?, auth_type=?, auth_config=?,
	              http_method=?, headers=?, query_params=?, request_body=?, data_path=?,
	              field_mappings=?, pagination_type=?, pagination_config=?,
	              updated_at=CURRENT_TIMESTAMP
	          WHERE id=?`

	_, err := m.db.Exec(query, conn.Name, conn.EndpointURL, conn.TimestampField,
		conn.AuthType, conn.AuthConfig, conn.HTTPMethod, conn.Headers, conn.QueryParams,
		conn.RequestBody, conn.DataPath, conn.FieldMappings, conn.PaginationType,
		conn.PaginationConfig, id)

	return err
}

// DeleteHTTPJSONConnection deletes an HTTP JSON connection
func (m *Manager) DeleteHTTPJSONConnection(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM http_json_connections WHERE id=?", id)
	return err
}

// ============================================================================
// Avro Schemas
// ============================================================================

// GetAvroSchemas returns all Avro schemas
func (m *Manager) GetAvroSchemas() ([]*AvroSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, topic_pattern, schema_name, schema_json, description, priority,
	                 is_active, created_at, updated_at
	          FROM avro_schemas WHERE is_active=1 ORDER BY priority DESC, created_at DESC`

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []*AvroSchema
	for rows.Next() {
		schema := &AvroSchema{}
		err := rows.Scan(&schema.ID, &schema.TopicPattern, &schema.SchemaName, &schema.SchemaJSON,
			&schema.Description, &schema.Priority, &schema.IsActive, &schema.CreatedAt, &schema.UpdatedAt)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	return schemas, rows.Err()
}

// AddAvroSchema adds a new Avro schema
func (m *Manager) AddAvroSchema(schema *AvroSchema) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate schema JSON
	var temp interface{}
	if err := json.Unmarshal([]byte(schema.SchemaJSON), &temp); err != nil {
		return 0, fmt.Errorf("invalid schema JSON: %w", err)
	}

	query := `INSERT INTO avro_schemas
	          (topic_pattern, schema_name, schema_json, description, priority, is_active)
	          VALUES (?, ?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, schema.TopicPattern, schema.SchemaName, schema.SchemaJSON,
		schema.Description, schema.Priority, schema.IsActive)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// UpdateAvroSchema updates an existing Avro schema
func (m *Manager) UpdateAvroSchema(id int64, schema *AvroSchema) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate schema JSON
	var temp interface{}
	if err := json.Unmarshal([]byte(schema.SchemaJSON), &temp); err != nil {
		return fmt.Errorf("invalid schema JSON: %w", err)
	}

	query := `UPDATE avro_schemas
	          SET topic_pattern=?, schema_name=?, schema_json=?, description=?,
	              priority=?, is_active=?, updated_at=CURRENT_TIMESTAMP
	          WHERE id=?`

	_, err := m.db.Exec(query, schema.TopicPattern, schema.SchemaName, schema.SchemaJSON,
		schema.Description, schema.Priority, schema.IsActive, id)

	return err
}

// DeleteAvroSchema deletes an Avro schema
func (m *Manager) DeleteAvroSchema(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM avro_schemas WHERE id=?", id)
	return err
}

// GetAvroSchemaForTopic finds the best matching Avro schema for a topic
func (m *Manager) GetAvroSchemaForTopic(topic string) (*AvroSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// For now, simple pattern matching (can be enhanced with regex)
	query := `SELECT id, topic_pattern, schema_name, schema_json, description, priority,
	                 is_active, created_at, updated_at
	          FROM avro_schemas
	          WHERE is_active=1 AND ? LIKE topic_pattern
	          ORDER BY priority DESC, LENGTH(topic_pattern) DESC
	          LIMIT 1`

	schema := &AvroSchema{}
	err := m.db.QueryRow(query, topic).Scan(&schema.ID, &schema.TopicPattern, &schema.SchemaName,
		&schema.SchemaJSON, &schema.Description, &schema.Priority, &schema.IsActive,
		&schema.CreatedAt, &schema.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return schema, nil
}

// ============================================================================
// Export Jobs
// ============================================================================

// GetExportJobs returns all export jobs
func (m *Manager) GetExportJobs() ([]*ExportJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, job_type, measurement, source_type, influx_connection_id,
	                 http_json_connection_id, storage_connection_id, cron_schedule, chunk_size,
	                 overlap_buffer, initial_export_mode, initial_start_date, initial_chunk_duration,
	                 retention_days, export_buffer_days, max_retries, retry_delay, is_active,
	                 created_at, updated_at
	          FROM export_jobs ORDER BY created_at DESC`

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*ExportJob
	for rows.Next() {
		job := &ExportJob{}
		err := rows.Scan(&job.ID, &job.Name, &job.JobType, &job.Measurement, &job.SourceType,
			&job.InfluxConnectionID, &job.HTTPJSONConnectionID, &job.StorageConnectionID,
			&job.CronSchedule, &job.ChunkSize, &job.OverlapBuffer, &job.InitialExportMode,
			&job.InitialStartDate, &job.InitialChunkDuration, &job.RetentionDays,
			&job.ExportBufferDays, &job.MaxRetries, &job.RetryDelay, &job.IsActive,
			&job.CreatedAt, &job.UpdatedAt)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// GetExportJobByID returns an export job by ID
func (m *Manager) GetExportJobByID(id int64) (*ExportJob, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, job_type, measurement, source_type, influx_connection_id,
	                 http_json_connection_id, storage_connection_id, cron_schedule, chunk_size,
	                 overlap_buffer, initial_export_mode, initial_start_date, initial_chunk_duration,
	                 retention_days, export_buffer_days, max_retries, retry_delay, is_active,
	                 created_at, updated_at
	          FROM export_jobs WHERE id=?`

	job := &ExportJob{}
	err := m.db.QueryRow(query, id).Scan(&job.ID, &job.Name, &job.JobType, &job.Measurement,
		&job.SourceType, &job.InfluxConnectionID, &job.HTTPJSONConnectionID,
		&job.StorageConnectionID, &job.CronSchedule, &job.ChunkSize, &job.OverlapBuffer,
		&job.InitialExportMode, &job.InitialStartDate, &job.InitialChunkDuration,
		&job.RetentionDays, &job.ExportBufferDays, &job.MaxRetries, &job.RetryDelay,
		&job.IsActive, &job.CreatedAt, &job.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return job, nil
}

// CreateExportJob creates a new export job
func (m *Manager) CreateExportJob(job *ExportJob) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO export_jobs
	          (name, job_type, measurement, source_type, influx_connection_id, http_json_connection_id,
	           storage_connection_id, cron_schedule, chunk_size, overlap_buffer, initial_export_mode,
	           initial_start_date, initial_chunk_duration, retention_days, export_buffer_days,
	           max_retries, retry_delay, is_active)
	          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, job.Name, job.JobType, job.Measurement, job.SourceType,
		job.InfluxConnectionID, job.HTTPJSONConnectionID, job.StorageConnectionID,
		job.CronSchedule, job.ChunkSize, job.OverlapBuffer, job.InitialExportMode,
		job.InitialStartDate, job.InitialChunkDuration, job.RetentionDays,
		job.ExportBufferDays, job.MaxRetries, job.RetryDelay, job.IsActive)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// UpdateExportJob updates an export job
func (m *Manager) UpdateExportJob(id int64, job *ExportJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `UPDATE export_jobs
	          SET name=?, job_type=?, measurement=?, source_type=?, influx_connection_id=?,
	              http_json_connection_id=?, storage_connection_id=?, cron_schedule=?,
	              chunk_size=?, overlap_buffer=?, initial_export_mode=?, initial_start_date=?,
	              initial_chunk_duration=?, retention_days=?, export_buffer_days=?,
	              max_retries=?, retry_delay=?, is_active=?, updated_at=CURRENT_TIMESTAMP
	          WHERE id=?`

	_, err := m.db.Exec(query, job.Name, job.JobType, job.Measurement, job.SourceType,
		job.InfluxConnectionID, job.HTTPJSONConnectionID, job.StorageConnectionID,
		job.CronSchedule, job.ChunkSize, job.OverlapBuffer, job.InitialExportMode,
		job.InitialStartDate, job.InitialChunkDuration, job.RetentionDays,
		job.ExportBufferDays, job.MaxRetries, job.RetryDelay, job.IsActive, id)

	return err
}

// DeleteExportJob deletes an export job
func (m *Manager) DeleteExportJob(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM export_jobs WHERE id=?", id)
	return err
}

// GetExportExecutions returns executions for a job
func (m *Manager) GetExportExecutions(jobID int64, limit int) ([]*ExportExecution, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, job_id, status, start_time, end_time, records_exported, error_message, created_at
	          FROM export_executions WHERE job_id=? ORDER BY start_time DESC LIMIT ?`

	rows, err := m.db.Query(query, jobID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var executions []*ExportExecution
	for rows.Next() {
		exec := &ExportExecution{}
		err := rows.Scan(&exec.ID, &exec.JobID, &exec.Status, &exec.StartTime, &exec.EndTime,
			&exec.RecordsExported, &exec.ErrorMessage, &exec.CreatedAt)
		if err != nil {
			return nil, err
		}
		executions = append(executions, exec)
	}

	return executions, rows.Err()
}

// CreateExportExecution creates a new export execution record
func (m *Manager) CreateExportExecution(exec *ExportExecution) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO export_executions (job_id, status, start_time, records_exported)
	          VALUES (?, ?, ?, ?)`

	result, err := m.db.Exec(query, exec.JobID, exec.Status, exec.StartTime, exec.RecordsExported)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// UpdateExportExecution updates an export execution record
func (m *Manager) UpdateExportExecution(id int64, exec *ExportExecution) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `UPDATE export_executions
	          SET status=?, end_time=?, records_exported=?, error_message=?
	          WHERE id=?`

	_, err := m.db.Exec(query, exec.Status, exec.EndTime, exec.RecordsExported, exec.ErrorMessage, id)
	return err
}

// Helper methods for getting connections by ID

func (m *Manager) GetInfluxConnectionByID(id int64) (*InfluxConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, version, host, port, database_name, username, password,
	                 token, org, bucket, database, ssl, is_active, created_at, updated_at
	          FROM influx_connections WHERE id=?`

	conn := &InfluxConnection{}
	err := m.db.QueryRow(query, id).Scan(&conn.ID, &conn.Name, &conn.Version, &conn.Host,
		&conn.Port, &conn.DatabaseName, &conn.Username, &conn.Password, &conn.Token,
		&conn.Org, &conn.Bucket, &conn.Database, &conn.SSL, &conn.IsActive,
		&conn.CreatedAt, &conn.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func (m *Manager) GetHTTPJSONConnectionByID(id int64) (*HTTPJSONConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, name, endpoint_url, timestamp_field, auth_type, auth_config,
	                 http_method, headers, query_params, request_body, data_path,
	                 field_mappings, pagination_type, pagination_config, is_active,
	                 created_at, updated_at
	          FROM http_json_connections WHERE id=?`

	conn := &HTTPJSONConnection{}
	err := m.db.QueryRow(query, id).Scan(&conn.ID, &conn.Name, &conn.EndpointURL,
		&conn.TimestampField, &conn.AuthType, &conn.AuthConfig, &conn.HTTPMethod,
		&conn.Headers, &conn.QueryParams, &conn.RequestBody, &conn.DataPath,
		&conn.FieldMappings, &conn.PaginationType, &conn.PaginationConfig,
		&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// ============================================================================
// API Tokens
// ============================================================================

// CreateAPIToken creates a new API token
func (m *Manager) CreateAPIToken(token *APIToken) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO api_tokens (token_hash, name, description, permissions, expires_at)
	          VALUES (?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, token.TokenHash, token.Name, token.Description,
		token.Permissions, token.ExpiresAt)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// GetAPITokens returns all API tokens
func (m *Manager) GetAPITokens() ([]*APIToken, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, token_hash, name, description, permissions, expires_at, last_used_at, created_at, updated_at
	          FROM api_tokens ORDER BY created_at DESC`

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []*APIToken
	for rows.Next() {
		token := &APIToken{}
		err := rows.Scan(&token.ID, &token.TokenHash, &token.Name, &token.Description,
			&token.Permissions, &token.ExpiresAt, &token.LastUsedAt, &token.CreatedAt, &token.UpdatedAt)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}

	return tokens, rows.Err()
}

// GetAPITokenByID returns an API token by ID
func (m *Manager) GetAPITokenByID(id int64) (*APIToken, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	query := `SELECT id, token_hash, name, description, permissions, expires_at, last_used_at, created_at, updated_at
	          FROM api_tokens WHERE id=?`

	token := &APIToken{}
	err := m.db.QueryRow(query, id).Scan(&token.ID, &token.TokenHash, &token.Name,
		&token.Description, &token.Permissions, &token.ExpiresAt, &token.LastUsedAt,
		&token.CreatedAt, &token.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return token, nil
}

// UpdateAPIToken updates an API token
func (m *Manager) UpdateAPIToken(id int64, token *APIToken) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `UPDATE api_tokens
	          SET token_hash=?, name=?, description=?, permissions=?, expires_at=?, updated_at=CURRENT_TIMESTAMP
	          WHERE id=?`

	_, err := m.db.Exec(query, token.TokenHash, token.Name, token.Description,
		token.Permissions, token.ExpiresAt, id)

	return err
}

// DeleteAPIToken deletes an API token
func (m *Manager) DeleteAPIToken(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM api_tokens WHERE id=?", id)
	return err
}

// ============================================================================
// Query History
// ============================================================================

// AddQueryHistory adds a new query history record
func (m *Manager) AddQueryHistory(history *QueryHistory) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	query := `INSERT INTO query_history (sql, database_name, row_count, duration_ms, status, error_message)
	          VALUES (?, ?, ?, ?, ?, ?)`

	result, err := m.db.Exec(query, history.SQL, history.DatabaseName, history.RowCount,
		history.DurationMs, history.Status, history.ErrorMessage)
	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// GetQueryHistory returns recent query history records
func (m *Manager) GetQueryHistory(limit int) ([]*QueryHistory, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	query := `SELECT id, sql, database_name, row_count, duration_ms, status, error_message, executed_at
	          FROM query_history ORDER BY executed_at DESC LIMIT ?`

	rows, err := m.db.Query(query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var histories []*QueryHistory
	for rows.Next() {
		h := &QueryHistory{}
		err := rows.Scan(&h.ID, &h.SQL, &h.DatabaseName, &h.RowCount, &h.DurationMs,
			&h.Status, &h.ErrorMessage, &h.ExecutedAt)
		if err != nil {
			return nil, err
		}
		histories = append(histories, h)
	}

	return histories, rows.Err()
}

// DeleteQueryHistory deletes a query history record
func (m *Manager) DeleteQueryHistory(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM query_history WHERE id=?", id)
	return err
}

// ClearQueryHistory deletes all query history records
func (m *Manager) ClearQueryHistory() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec("DELETE FROM query_history")
	return err
}
