package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

// Config represents the complete Arc Core configuration
type Config struct {
	Server     ServerConfig     `toml:"server"`
	Auth       AuthConfig       `toml:"auth"`
	QueryCache QueryCacheConfig `toml:"query_cache"`
	DuckDB     DuckDBConfig     `toml:"duckdb"`
	Delete     DeleteConfig     `toml:"delete"`
	Ingestion  IngestionConfig  `toml:"ingestion"`
	WAL        WALConfig        `toml:"wal"`
	Storage    StorageConfig    `toml:"storage"`
	Logging    LoggingConfig    `toml:"logging"`
	CORS       CORSConfig       `toml:"cors"`
	Compaction CompactionConfig `toml:"compaction"`
	Monitoring MonitoringConfig `toml:"monitoring"`
	Advanced   AdvancedConfig   `toml:"advanced"`
	GoRuntime  GoRuntimeConfig  `toml:"go_runtime"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Host              string `toml:"host"`
	Port              int    `toml:"port"`
	Workers           int    `toml:"workers"`
	WorkerTimeout     int    `toml:"worker_timeout"`
	GracefulTimeout   int    `toml:"graceful_timeout"`
	MaxRequests       int    `toml:"max_requests"`
	MaxRequestsJitter int    `toml:"max_requests_jitter"`
	WorkerConnections int    `toml:"worker_connections"`
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Enabled      bool   `toml:"enabled"`
	DefaultToken string `toml:"default_token"`
	Allowlist    string `toml:"allowlist"`
}

// QueryCacheConfig holds query cache configuration
type QueryCacheConfig struct {
	Enabled                bool `toml:"enabled"`
	TTLSeconds             int  `toml:"ttl_seconds"`
	MaxSizeMB              int  `toml:"max_size_mb"`
	MaxEntries             int  `toml:"max_entries"`
	CleanupIntervalSeconds int  `toml:"cleanup_interval_seconds"`
}

// DuckDBConfig holds DuckDB configuration
type DuckDBConfig struct {
	PoolSize           int    `toml:"pool_size"`
	MaxQueueSize       int    `toml:"max_queue_size"`
	QueryTimeout       int    `toml:"query_timeout"`
	StreamingThreshold int    `toml:"streaming_threshold"`
	MemoryLimit        string `toml:"memory_limit"`
	Threads            int    `toml:"threads"`
	TempDirectory      string `toml:"temp_directory"`
	EnableObjectCache  bool   `toml:"enable_object_cache"`
}

// DeleteConfig holds delete operations configuration
type DeleteConfig struct {
	Enabled                bool `toml:"enabled"`
	ConfirmationThreshold  int  `toml:"confirmation_threshold"`
	MaxRowsPerDelete       int  `toml:"max_rows_per_delete"`
	TombstoneRetentionDays int  `toml:"tombstone_retention_days"`
	AuditEnabled           bool `toml:"audit_enabled"`
}

// IngestionConfig holds data ingestion configuration
type IngestionConfig struct {
	BufferSize       int    `toml:"buffer_size"`
	BufferAgeSeconds int    `toml:"buffer_age_seconds"`
	Compression      string `toml:"compression"`
}

// WALConfig holds Write-Ahead Log configuration
type WALConfig struct {
	Enabled       bool   `toml:"enabled"`
	Dir           string `toml:"dir"`
	SyncMode      string `toml:"sync_mode"`
	MaxSizeMB     int    `toml:"max_size_mb"`
	MaxAgeSeconds int    `toml:"max_age_seconds"`
}

// StorageConfig holds storage backend configuration
type StorageConfig struct {
	Backend string      `toml:"backend"`
	Local   LocalConfig `toml:"local"`
	MinIO   MinIOConfig `toml:"minio"`
	S3      S3Config    `toml:"s3"`
	GCS     GCSConfig   `toml:"gcs"`
}

// LocalConfig holds local filesystem configuration
type LocalConfig struct {
	BasePath string `toml:"base_path"`
	Database string `toml:"database"`
}

// MinIOConfig holds MinIO configuration
type MinIOConfig struct {
	Endpoint  string `toml:"endpoint"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
	Database  string `toml:"database"`
	UseSSL    bool   `toml:"use_ssl"`
}

// S3Config holds AWS S3 configuration
type S3Config struct {
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
	Region    string `toml:"region"`
	Database  string `toml:"database"`
}

// GCSConfig holds Google Cloud Storage configuration
type GCSConfig struct {
	Bucket          string `toml:"bucket"`
	ProjectID       string `toml:"project_id"`
	CredentialsFile string `toml:"credentials_file"`
	Database        string `toml:"database"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level        string `toml:"level"`
	Format       string `toml:"format"`
	IncludeTrace bool   `toml:"include_trace"`
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	Origins string `toml:"origins"`
}

// CompactionConfig holds compaction configuration
type CompactionConfig struct {
	Enabled           bool   `toml:"enabled"`
	MinAgeHours       int    `toml:"min_age_hours"`
	MinFiles          int    `toml:"min_files"`
	TargetFileSizeMB  int    `toml:"target_file_size_mb"`
	Schedule          string `toml:"schedule"`
	MaxConcurrentJobs int    `toml:"max_concurrent_jobs"`
	LockTTLHours      int    `toml:"lock_ttl_hours"`
	TempDir           string `toml:"temp_dir"`
	Compression       string `toml:"compression"`
	CompressionLevel  int    `toml:"compression_level"`
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	Enabled            bool `toml:"enabled"`
	CollectionInterval int  `toml:"collection_interval"`
}

// AdvancedConfig holds advanced configuration
type AdvancedConfig struct {
	Experimental       bool `toml:"experimental"`
	ArrowFlightEnabled bool `toml:"arrow_flight_enabled"`
	ArrowFlightPort    int  `toml:"arrow_flight_port"`
}

// GoRuntimeConfig holds Go-specific runtime configuration
type GoRuntimeConfig struct {
	GoMaxProcs      int `toml:"gomaxprocs"`
	GCTargetPercent int `toml:"gc_target_percent"`
	MaxPendingOps   int `toml:"max_pending_ops"`
}

// Load loads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	// Load .env file if it exists
	_ = godotenv.Load()

	// Set default configuration
	config := &Config{
		Server: ServerConfig{
			Host:              "0.0.0.0",
			Port:              8000,
			Workers:           4,
			WorkerTimeout:     120,
			GracefulTimeout:   60,
			MaxRequests:       50000,
			MaxRequestsJitter: 5000,
			WorkerConnections: 1000,
		},
		Auth: AuthConfig{
			Enabled:      false,
			DefaultToken: "",
			Allowlist:    "/health,/ready,/docs,/openapi.json,/auth/verify",
		},
		QueryCache: QueryCacheConfig{
			Enabled:    true,
			TTLSeconds: 60,
			MaxSizeMB:  100,
			MaxEntries: 10,
		},
		DuckDB: DuckDBConfig{
			PoolSize:           15,
			MaxQueueSize:       300,
			QueryTimeout:       300,
			StreamingThreshold: 100000,
			MemoryLimit:        "8GB",
			Threads:            14,
			TempDirectory:      "./data/duckdb_tmp",
			EnableObjectCache:  true,
		},
		Delete: DeleteConfig{
			Enabled:                true,
			ConfirmationThreshold:  10000,
			MaxRowsPerDelete:       1000000,
			TombstoneRetentionDays: 0,
			AuditEnabled:           true,
		},
		Ingestion: IngestionConfig{
			BufferSize:       200000,
			BufferAgeSeconds: 10,
			Compression:      "snappy",
		},
		WAL: WALConfig{
			Enabled:       false,
			Dir:           "./data/wal",
			SyncMode:      "fsync",
			MaxSizeMB:     100,
			MaxAgeSeconds: 3600,
		},
		Storage: StorageConfig{
			Backend: "local",
			Local: LocalConfig{
				BasePath: "./data/arc",
				Database: "default",
			},
			MinIO: MinIOConfig{
				Endpoint:  "http://localhost:9000",
				AccessKey: "minioadmin",
				SecretKey: "minioadmin",
				Bucket:    "arc",
				Database:  "default",
				UseSSL:    false,
			},
		},
		Logging: LoggingConfig{
			Level:        "INFO",
			Format:       "structured",
			IncludeTrace: false,
		},
		CORS: CORSConfig{
			Origins: "http://localhost:3000,http://atila:3000",
		},
		Compaction: CompactionConfig{
			Enabled:           true,
			MinAgeHours:       0,
			MinFiles:          10,
			TargetFileSizeMB:  512,
			Schedule:          "5 * * * *",
			MaxConcurrentJobs: 2,
			LockTTLHours:      2,
			TempDir:           "./data/compaction",
			Compression:       "zstd",
			CompressionLevel:  3,
		},
		Monitoring: MonitoringConfig{
			Enabled:            false,
			CollectionInterval: 30,
		},
		Advanced: AdvancedConfig{
			Experimental:       false,
			ArrowFlightEnabled: false,
			ArrowFlightPort:    8081,
		},
		GoRuntime: GoRuntimeConfig{
			GoMaxProcs:      0, // Auto-detect
			GCTargetPercent: 50,
			MaxPendingOps:   1000,
		},
	}

	// Load from TOML file if it exists
	if configPath != "" {
		if _, err := toml.DecodeFile(configPath, config); err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to decode config file %s: %w", configPath, err)
			}
		}
	}

	// Override with environment variables
	if err := overrideWithEnv(config); err != nil {
		return nil, fmt.Errorf("failed to override config with environment variables: %w", err)
	}

	// Validate configuration
	if err := validate(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return config, nil
}

// overrideWithEnv overrides configuration with environment variables
func overrideWithEnv(config *Config) error {
	// Server settings
	if val := os.Getenv("ARC_SERVER_HOST"); val != "" {
		config.Server.Host = val
	}
	if val := os.Getenv("ARC_SERVER_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			config.Server.Port = port
		}
	}
	if val := os.Getenv("ARC_SERVER_WORKERS"); val != "" {
		if workers, err := strconv.Atoi(val); err == nil {
			config.Server.Workers = workers
		}
	}

	// Auth settings
	if val := os.Getenv("ARC_AUTH_ENABLED"); val != "" {
		if enabled, err := strconv.ParseBool(val); err == nil {
			config.Auth.Enabled = enabled
		}
	}
	if val := os.Getenv("ARC_AUTH_DEFAULT_TOKEN"); val != "" {
		config.Auth.DefaultToken = val
	}

	// Storage settings
	if val := os.Getenv("ARC_STORAGE_BACKEND"); val != "" {
		config.Storage.Backend = val
	}

	// Local storage
	if val := os.Getenv("ARC_STORAGE_LOCAL_BASE_PATH"); val != "" {
		config.Storage.Local.BasePath = val
	}
	if val := os.Getenv("ARC_STORAGE_LOCAL_DATABASE"); val != "" {
		config.Storage.Local.Database = val
	}

	// MinIO settings
	if val := os.Getenv("ARC_STORAGE_MINIO_ENDPOINT"); val != "" {
		config.Storage.MinIO.Endpoint = val
	}
	if val := os.Getenv("ARC_STORAGE_MINIO_ACCESS_KEY"); val != "" {
		config.Storage.MinIO.AccessKey = val
	}
	if val := os.Getenv("ARC_STORAGE_MINIO_SECRET_KEY"); val != "" {
		config.Storage.MinIO.SecretKey = val
	}
	if val := os.Getenv("ARC_STORAGE_MINIO_BUCKET"); val != "" {
		config.Storage.MinIO.Bucket = val
	}
	if val := os.Getenv("ARC_STORAGE_MINIO_DATABASE"); val != "" {
		config.Storage.MinIO.Database = val
	}
	if val := os.Getenv("ARC_STORAGE_MINIO_USE_SSL"); val != "" {
		if useSSL, err := strconv.ParseBool(val); err == nil {
			config.Storage.MinIO.UseSSL = useSSL
		}
	}

	// Ingestion settings
	if val := os.Getenv("ARC_INGESTION_BUFFER_SIZE"); val != "" {
		if size, err := strconv.Atoi(val); err == nil {
			config.Ingestion.BufferSize = size
		}
	}
	if val := os.Getenv("ARC_INGESTION_BUFFER_AGE_SECONDS"); val != "" {
		if age, err := strconv.Atoi(val); err == nil {
			config.Ingestion.BufferAgeSeconds = age
		}
	}

	// WAL settings
	if val := os.Getenv("ARC_WAL_ENABLED"); val != "" {
		if enabled, err := strconv.ParseBool(val); err == nil {
			config.WAL.Enabled = enabled
		}
	}
	if val := os.Getenv("ARC_WAL_DIR"); val != "" {
		config.WAL.Dir = val
	}
	if val := os.Getenv("ARC_WAL_SYNC_MODE"); val != "" {
		config.WAL.SyncMode = val
	}

	// Logging settings
	if val := os.Getenv("ARC_LOGGING_LEVEL"); val != "" {
		config.Logging.Level = strings.ToUpper(val)
	}
	if val := os.Getenv("ARC_LOGGING_FORMAT"); val != "" {
		config.Logging.Format = val
	}

	// Go runtime settings
	if val := os.Getenv("ARC_GO_RUNTIME_GOMAXPROCS"); val != "" {
		if gmp, err := strconv.Atoi(val); err == nil {
			config.GoRuntime.GoMaxProcs = gmp
		}
	}
	if val := os.Getenv("ARC_GO_RUNTIME_GC_TARGET_PERCENT"); val != "" {
		if gcp, err := strconv.Atoi(val); err == nil {
			config.GoRuntime.GCTargetPercent = gcp
		}
	}

	return nil
}

// validate validates the configuration
func validate(config *Config) error {
	// Validate server configuration
	if config.Server.Port < 1 || config.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", config.Server.Port)
	}
	if config.Server.Workers < 0 {
		return fmt.Errorf("invalid number of workers: %d", config.Server.Workers)
	}

	// Validate storage backend
	validBackends := []string{"local", "minio", "s3", "gcs", "ceph"}
	backendValid := false
	for _, b := range validBackends {
		if config.Storage.Backend == b {
			backendValid = true
			break
		}
	}
	if !backendValid {
		return fmt.Errorf("invalid storage backend: %s", config.Storage.Backend)
	}

	// Validate MinIO configuration if MinIO backend
	if config.Storage.Backend == "minio" {
		if config.Storage.MinIO.Endpoint == "" {
			return fmt.Errorf("minio endpoint is required when using minio backend")
		}
		if config.Storage.MinIO.AccessKey == "" {
			return fmt.Errorf("minio access key is required when using minio backend")
		}
		if config.Storage.MinIO.SecretKey == "" {
			return fmt.Errorf("minio secret key is required when using minio backend")
		}
		if config.Storage.MinIO.Bucket == "" {
			return fmt.Errorf("minio bucket is required when using minio backend")
		}
	}

	// Validate ingestion configuration
	if config.Ingestion.BufferSize <= 0 {
		return fmt.Errorf("buffer size must be positive: %d", config.Ingestion.BufferSize)
	}
	if config.Ingestion.BufferAgeSeconds <= 0 {
		return fmt.Errorf("buffer age seconds must be positive: %d", config.Ingestion.BufferAgeSeconds)
	}

	// Validate WAL configuration
	if config.WAL.Enabled {
		validSyncModes := []string{"fsync", "fdatasync", "async"}
		syncModeValid := false
		for _, mode := range validSyncModes {
			if config.WAL.SyncMode == mode {
				syncModeValid = true
				break
			}
		}
		if !syncModeValid {
			return fmt.Errorf("invalid WAL sync mode: %s", config.WAL.SyncMode)
		}
	}

	// Validate logging level
	validLogLevels := []string{"DEBUG", "INFO", "WARNING", "ERROR"}
	logLevelValid := false
	for _, level := range validLogLevels {
		if config.Logging.Level == level {
			logLevelValid = true
			break
		}
	}
	if !logLevelValid {
		return fmt.Errorf("invalid logging level: %s", config.Logging.Level)
	}

	return nil
}

// GetListenAddress returns the server listen address
func (c *Config) GetListenAddress() string {
	return fmt.Sprintf("%s:%d", c.Server.Host, c.Server.Port)
}

// IsAuthEnabled returns true if authentication is enabled
func (c *Config) IsAuthEnabled() bool {
	return c.Auth.Enabled && c.Auth.DefaultToken != ""
}

// GetDatabase returns the current database name based on storage backend
func (c *Config) GetDatabase() string {
	switch c.Storage.Backend {
	case "local":
		return c.Storage.Local.Database
	case "minio":
		return c.Storage.MinIO.Database
	case "s3":
		return c.Storage.S3.Database
	case "gcs":
		return c.Storage.GCS.Database
	default:
		return "default"
	}
}

// GetBufferFlushDuration returns the buffer flush duration
func (c *Config) GetBufferFlushDuration() time.Duration {
	return time.Duration(c.Ingestion.BufferAgeSeconds) * time.Second
}

// GetWALSyncMode returns the WAL sync mode with default
func (c *Config) GetWALSyncMode() string {
	if c.WAL.SyncMode == "" {
		return "fsync"
	}
	return c.WAL.SyncMode
}

// GetLogrusLevel returns the logrus log level
func (c *Config) GetLogrusLevel() logrus.Level {
	switch c.Logging.Level {
	case "DEBUG":
		return logrus.DebugLevel
	case "INFO":
		return logrus.InfoLevel
	case "WARNING":
		return logrus.WarnLevel
	case "ERROR":
		return logrus.ErrorLevel
	default:
		return logrus.InfoLevel
	}
}

// IsStructuredLogging returns true if structured logging is enabled
func (c *Config) IsStructuredLogging() bool {
	return c.Logging.Format == "structured"
}

// GetCORSOrigins returns the CORS origins as a slice
func (c *Config) GetCORSOrigins() []string {
	if c.CORS.Origins == "" {
		return []string{}
	}
	return strings.Split(c.CORS.Origins, ",")
}

// GetAuthAllowlist returns the authentication allowlist as a slice
func (c *Config) GetAuthAllowlist() []string {
	if c.Auth.Allowlist == "" {
		return []string{}
	}
	return strings.Split(c.Auth.Allowlist, ",")
}
