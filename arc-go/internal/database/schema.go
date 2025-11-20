package database

const (
	// Schema version for migrations
	SchemaVersion = 1

	// CreateInfluxConnectionsTable creates the InfluxDB connections table
	CreateInfluxConnectionsTable = `
		CREATE TABLE IF NOT EXISTS influx_connections (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			version TEXT NOT NULL,  -- '1x', '2x', '3x', or 'timescale'
			host TEXT NOT NULL,
			port INTEGER NOT NULL,
			database_name TEXT,     -- For 1.x and TimescaleDB
			username TEXT,          -- For 1.x and TimescaleDB
			password TEXT,          -- For 1.x and TimescaleDB
			token TEXT,             -- For 2.x and 3.x
			org TEXT,               -- For 2.x and 3.x
			bucket TEXT,            -- For 2.x
			database TEXT,          -- For 3.x
			ssl BOOLEAN DEFAULT FALSE,
			is_active BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	// CreateStorageConnectionsTable creates the storage connections table
	CreateStorageConnectionsTable = `
		CREATE TABLE IF NOT EXISTS storage_connections (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			backend TEXT NOT NULL,  -- 'local', 'minio', 's3', 'ceph', or 'gcs'
			endpoint TEXT,          -- For MinIO/Ceph
			access_key TEXT,        -- For S3/MinIO (optional for GCS)
			secret_key TEXT,        -- For S3/MinIO (optional for GCS)
			bucket TEXT,            -- For object storage (nullable for local)
			base_path TEXT,         -- For local filesystem storage
			database TEXT DEFAULT 'default',
			region TEXT DEFAULT 'us-east-1',
			ssl BOOLEAN DEFAULT FALSE,
			use_directory_bucket BOOLEAN DEFAULT FALSE,  -- For S3 Directory Buckets
			availability_zone TEXT,                      -- For S3 Directory Buckets
			project_id TEXT,                            -- For GCS
			credentials_json TEXT,                      -- For GCS service account JSON
			credentials_file TEXT,                      -- For GCS service account file path
			hmac_key_id TEXT,                           -- For GCS HMAC authentication
			hmac_secret TEXT,                           -- For GCS HMAC authentication
			is_active BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	// CreateHTTPJSONConnectionsTable creates the HTTP JSON connections table
	CreateHTTPJSONConnectionsTable = `
		CREATE TABLE IF NOT EXISTS http_json_connections (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			endpoint_url TEXT NOT NULL,
			timestamp_field TEXT NOT NULL,
			auth_type TEXT,         -- 'none', 'bearer', 'basic', 'api_key'
			auth_config TEXT,       -- JSON config for auth
			http_method TEXT DEFAULT 'GET',
			headers TEXT,           -- JSON for additional headers
			query_params TEXT,      -- JSON for query parameters
			request_body TEXT,      -- Template for POST body
			data_path TEXT,         -- JSON path to data array
			field_mappings TEXT,    -- JSON for field mappings
			pagination_type TEXT,   -- 'none', 'offset', 'cursor', 'page'
			pagination_config TEXT, -- JSON for pagination config
			is_active BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	// CreateExportJobsTable creates the export jobs table
	CreateExportJobsTable = `
		CREATE TABLE IF NOT EXISTS export_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT UNIQUE NOT NULL,
			job_type TEXT NOT NULL,  -- 'measurement' or 'database'
			measurement TEXT,        -- NULL for database-wide
			source_type TEXT NOT NULL DEFAULT 'influx',  -- 'influx', 'http_json'
			influx_connection_id INTEGER,
			http_json_connection_id INTEGER,
			storage_connection_id INTEGER NOT NULL,
			cron_schedule TEXT NOT NULL,
			chunk_size TEXT NOT NULL,  -- '1h', '1d', '1w', etc.
			overlap_buffer TEXT DEFAULT '5m',
			initial_export_mode TEXT DEFAULT 'full',
			initial_start_date TEXT,
			initial_chunk_duration TEXT DEFAULT '1d',
			retention_days INTEGER DEFAULT 365,
			export_buffer_days INTEGER DEFAULT 7,
			max_retries INTEGER DEFAULT 3,
			retry_delay INTEGER DEFAULT 300,
			is_active BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (influx_connection_id) REFERENCES influx_connections (id),
			FOREIGN KEY (http_json_connection_id) REFERENCES http_json_connections (id),
			FOREIGN KEY (storage_connection_id) REFERENCES storage_connections (id)
		)`

	// CreateExportStateTable creates the export state tracking table
	CreateExportStateTable = `
		CREATE TABLE IF NOT EXISTS export_state (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			measurement TEXT NOT NULL,
			last_export_time TIMESTAMP,
			total_records_exported INTEGER DEFAULT 0,
			last_run_status TEXT DEFAULT 'pending',
			last_run_start TIMESTAMP,
			last_run_end TIMESTAMP,
			error_message TEXT,
			retry_count INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (job_id) REFERENCES export_jobs (id),
			UNIQUE(job_id, measurement)
		)`

	// CreateExportExecutionsTable creates the export executions table
	CreateExportExecutionsTable = `
		CREATE TABLE IF NOT EXISTS export_executions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id INTEGER NOT NULL,
			status TEXT NOT NULL,  -- 'running', 'completed', 'failed', 'cancelled'
			start_time TIMESTAMP NOT NULL,
			end_time TIMESTAMP,
			records_exported INTEGER DEFAULT 0,
			error_message TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (job_id) REFERENCES export_jobs (id)
		)`

	// CreateAvroSchemasTable creates the Avro schemas table
	CreateAvroSchemasTable = `
		CREATE TABLE IF NOT EXISTS avro_schemas (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			topic_pattern TEXT NOT NULL,
			schema_name TEXT NOT NULL,
			schema_json TEXT NOT NULL,
			description TEXT,
			priority INTEGER DEFAULT 0,
			is_active BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	// CreateAPITokensTable creates the API tokens table for advanced auth
	CreateAPITokensTable = `
		CREATE TABLE IF NOT EXISTS api_tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token_hash TEXT UNIQUE NOT NULL,
			name TEXT UNIQUE NOT NULL,
			description TEXT,
			permissions TEXT,  -- JSON array of permissions
			expires_at TIMESTAMP,
			last_used_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	// CreateQueryHistoryTable creates the query history table
	CreateQueryHistoryTable = `
		CREATE TABLE IF NOT EXISTS query_history (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			sql TEXT NOT NULL,
			database_name TEXT,
			row_count INTEGER DEFAULT 0,
			duration_ms INTEGER DEFAULT 0,
			status TEXT NOT NULL,  -- 'success', 'error'
			error_message TEXT,
			executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`

	// EnableWALMode enables Write-Ahead Logging for better concurrency
	EnableWALMode = `PRAGMA journal_mode=WAL`

	// SetBusyTimeout sets busy timeout for lock handling
	SetBusyTimeout = `PRAGMA busy_timeout=5000`
)

// AllTables returns all table creation statements in order
func AllTables() []string {
	return []string{
		CreateInfluxConnectionsTable,
		CreateStorageConnectionsTable,
		CreateHTTPJSONConnectionsTable,
		CreateExportJobsTable,
		CreateExportStateTable,
		CreateExportExecutionsTable,
		CreateAvroSchemasTable,
		CreateAPITokensTable,
		CreateQueryHistoryTable,
	}
}
