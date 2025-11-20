package database

import (
	"time"
)

// InfluxConnection represents an InfluxDB data source connection
type InfluxConnection struct {
	ID           int64     `json:"id"`
	Name         string    `json:"name"`
	Version      string    `json:"version"` // '1x', '2x', '3x', 'timescale'
	Host         string    `json:"host"`
	Port         int       `json:"port"`
	DatabaseName *string   `json:"database_name,omitempty"` // For 1.x and TimescaleDB
	Username     *string   `json:"username,omitempty"`
	Password     *string   `json:"password,omitempty"`
	Token        *string   `json:"token,omitempty"` // For 2.x and 3.x
	Org          *string   `json:"org,omitempty"`
	Bucket       *string   `json:"bucket,omitempty"`
	Database     *string   `json:"database,omitempty"` // For 3.x
	SSL          bool      `json:"ssl"`
	IsActive     bool      `json:"is_active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// StorageConnection represents a storage backend connection
type StorageConnection struct {
	ID                 int64     `json:"id"`
	Name               string    `json:"name"`
	Backend            string    `json:"backend"` // 'local', 'minio', 's3', 'ceph', 'gcs'
	Endpoint           *string   `json:"endpoint,omitempty"`
	AccessKey          *string   `json:"access_key,omitempty"`
	SecretKey          *string   `json:"secret_key,omitempty"`
	Bucket             *string   `json:"bucket,omitempty"`
	BasePath           *string   `json:"base_path,omitempty"`
	Database           string    `json:"database"`
	Region             string    `json:"region"`
	SSL                bool      `json:"ssl"`
	UseDirectoryBucket bool      `json:"use_directory_bucket"`
	AvailabilityZone   *string   `json:"availability_zone,omitempty"`
	ProjectID          *string   `json:"project_id,omitempty"`
	CredentialsJSON    *string   `json:"credentials_json,omitempty"`
	CredentialsFile    *string   `json:"credentials_file,omitempty"`
	HMACKeyID          *string   `json:"hmac_key_id,omitempty"`
	HMACSecret         *string   `json:"hmac_secret,omitempty"`
	IsActive           bool      `json:"is_active"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// HTTPJSONConnection represents an HTTP JSON data source
type HTTPJSONConnection struct {
	ID               int64     `json:"id"`
	Name             string    `json:"name"`
	EndpointURL      string    `json:"endpoint_url"`
	TimestampField   string    `json:"timestamp_field"`
	AuthType         *string   `json:"auth_type,omitempty"`
	AuthConfig       *string   `json:"auth_config,omitempty"` // JSON
	HTTPMethod       string    `json:"http_method"`
	Headers          *string   `json:"headers,omitempty"`      // JSON
	QueryParams      *string   `json:"query_params,omitempty"` // JSON
	RequestBody      *string   `json:"request_body,omitempty"`
	DataPath         *string   `json:"data_path,omitempty"`
	FieldMappings    *string   `json:"field_mappings,omitempty"` // JSON
	PaginationType   *string   `json:"pagination_type,omitempty"`
	PaginationConfig *string   `json:"pagination_config,omitempty"` // JSON
	IsActive         bool      `json:"is_active"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

// ExportJob represents a scheduled export job
type ExportJob struct {
	ID                   int64     `json:"id"`
	Name                 string    `json:"name"`
	JobType              string    `json:"job_type"` // 'measurement' or 'database'
	Measurement          *string   `json:"measurement,omitempty"`
	SourceType           string    `json:"source_type"` // 'influx', 'http_json'
	InfluxConnectionID   *int64    `json:"influx_connection_id,omitempty"`
	HTTPJSONConnectionID *int64    `json:"http_json_connection_id,omitempty"`
	StorageConnectionID  int64     `json:"storage_connection_id"`
	CronSchedule         string    `json:"cron_schedule"`
	ChunkSize            string    `json:"chunk_size"`
	OverlapBuffer        string    `json:"overlap_buffer"`
	InitialExportMode    string    `json:"initial_export_mode"`
	InitialStartDate     *string   `json:"initial_start_date,omitempty"`
	InitialChunkDuration string    `json:"initial_chunk_duration"`
	RetentionDays        int       `json:"retention_days"`
	ExportBufferDays     int       `json:"export_buffer_days"`
	MaxRetries           int       `json:"max_retries"`
	RetryDelay           int       `json:"retry_delay"`
	IsActive             bool      `json:"is_active"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
}

// ExportState tracks the state of an export job
type ExportState struct {
	ID                   int64      `json:"id"`
	JobID                int64      `json:"job_id"`
	Measurement          string     `json:"measurement"`
	LastExportTime       *time.Time `json:"last_export_time,omitempty"`
	TotalRecordsExported int64      `json:"total_records_exported"`
	LastRunStatus        string     `json:"last_run_status"`
	LastRunStart         *time.Time `json:"last_run_start,omitempty"`
	LastRunEnd           *time.Time `json:"last_run_end,omitempty"`
	ErrorMessage         *string    `json:"error_message,omitempty"`
	RetryCount           int        `json:"retry_count"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
}

// ExportExecution tracks individual job executions
type ExportExecution struct {
	ID              int64      `json:"id"`
	JobID           int64      `json:"job_id"`
	Status          string     `json:"status"` // 'running', 'completed', 'failed', 'cancelled'
	StartTime       time.Time  `json:"start_time"`
	EndTime         *time.Time `json:"end_time,omitempty"`
	RecordsExported int64      `json:"records_exported"`
	ErrorMessage    *string    `json:"error_message,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// AvroSchema represents an Avro schema for topic pattern matching
type AvroSchema struct {
	ID           int64     `json:"id"`
	TopicPattern string    `json:"topic_pattern"`
	SchemaName   string    `json:"schema_name"`
	SchemaJSON   string    `json:"schema_json"`
	Description  *string   `json:"description,omitempty"`
	Priority     int       `json:"priority"`
	IsActive     bool      `json:"is_active"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// APIToken represents an API token with permissions
type APIToken struct {
	ID          int64      `json:"id"`
	TokenHash   string     `json:"token_hash"`
	Name        string     `json:"name"`
	Description *string    `json:"description,omitempty"`
	Permissions *string    `json:"permissions,omitempty"` // JSON array
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// QueryHistory represents a SQL query execution history record
type QueryHistory struct {
	ID           int64     `json:"id"`
	SQL          string    `json:"sql"`
	DatabaseName *string   `json:"database_name,omitempty"`
	RowCount     int64     `json:"row_count"`
	DurationMs   int64     `json:"duration_ms"`
	Status       string    `json:"status"` // 'success', 'error'
	ErrorMessage *string   `json:"error_message,omitempty"`
	ExecutedAt   time.Time `json:"executed_at"`
}

// ConnectionTestResult represents the result of a connection test
type ConnectionTestResult struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
}
