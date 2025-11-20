package main

import (
	"context"
	"flag"
	"fmt"
	v2 "github.com/arc-core/arc-go/api/v1"
	"io"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/arc-core/arc-go/internal/auth"
	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/arc-core/arc-go/internal/monitoring"
	"github.com/arc-core/arc-go/internal/query"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/arc-core/arc-go/internal/wal"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

var (
	configFile = flag.String("config", "arc.conf", "Path to configuration file")
	version    = flag.Bool("version", false, "Show version information")
	help       = flag.Bool("help", false, "Show help information")
)

const (
	AppName    = "Arc Core"
	AppVersion = "1.0.0"
	AppDesc    = "High-performance time-series data warehouse"
)

func main() {
	flag.Parse()

	if *help {
		showHelp()
		return
	}

	if *version {
		showVersion()
		return
	}

	// Initialize logger
	logger := logrus.New()

	// Load configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		logger.WithError(err).Fatal("Failed to load configuration")
	}

	// Configure logger based on config
	configureLogger(logger, cfg)

	// Configure Go runtime
	configureGoRuntime(cfg)

	logger.WithFields(logrus.Fields{
		"version":     AppVersion,
		"config":      *configFile,
		"workers":     cfg.Server.Workers,
		"go_maxprocs": runtime.GOMAXPROCS(0),
	}).Info("Starting Arc Core")

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize storage backend
	storageBackend, err := initializeStorage(cfg, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize storage backend")
	}
	defer storageBackend.Close()

	// Initialize WAL if enabled
	var walInstance *wal.WAL
	if cfg.WAL.Enabled {
		pathBuilder := storage.NewDefaultPathBuilder()
		walInstance, err = wal.NewWAL(&cfg.WAL, storageBackend, pathBuilder, logger)
		if err != nil {
			logger.WithError(err).Fatal("Failed to initialize WAL")
		}
		defer walInstance.Close()

		// Perform WAL recovery on startup
		logger.Info("Starting WAL recovery...")
		recoveryHandler := &walRecoveryHandler{
			storage:     storageBackend,
			pathBuilder: pathBuilder,
			logger:      logger,
		}
		if err := walInstance.Recover(ctx, recoveryHandler); err != nil {
			logger.WithError(err).Warn("WAL recovery completed with errors")
		} else {
			logger.Info("WAL recovery completed successfully")
		}
	}

	// Initialize monitoring
	monitorCfg := &monitoring.MonitoringConfig{
		Enabled:            cfg.Monitoring.Enabled,
		CollectionInterval: time.Duration(cfg.Monitoring.CollectionInterval) * time.Second,
		MetricsPath:        "/metrics",
		Port:               9090,
	}
	monitor := monitoring.NewMonitor(monitorCfg, logger)
	go monitor.Start(ctx)

	// Initialize HTTP server
	server := initializeServer(cfg, storageBackend, walInstance, monitor, logger)

	// Start server in goroutine
	go func() {
		// Wait for interrupt signal
		waitForShutdown(server, cancel, logger)
	}()

	logger.WithField("address", cfg.GetListenAddress()).Info("Starting HTTP server")

	// Configure gin mode
	if cfg.Logging.Level == "DEBUG" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.WithError(err).Fatal("HTTP server failed")
	}

	logger.Info("Arc Core stopped")
}

// configureLogger configures the logger based on configuration
func configureLogger(logger *logrus.Logger, cfg *config.Config) {
	logger.SetLevel(cfg.GetLogrusLevel())

	if cfg.IsStructuredLogging() {
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
		})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
		})
	}

	if cfg.Logging.IncludeTrace {
		logger.SetReportCaller(true)
	}
}

// configureGoRuntime configures Go runtime settings
func configureGoRuntime(cfg *config.Config) {
	// Set GOMAXPROCS
	if cfg.GoRuntime.GoMaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.GoRuntime.GoMaxProcs)
	}

	// Set GC target percentage
	if cfg.GoRuntime.GCTargetPercent > 0 && cfg.GoRuntime.GCTargetPercent <= 100 {
		debug.SetGCPercent(cfg.GoRuntime.GCTargetPercent)
	}
}

// initializeStorage creates and initializes the storage backend
func initializeStorage(cfg *config.Config, logger *logrus.Logger) (storage.Backend, error) {
	switch cfg.Storage.Backend {
	case "local":
		localCfg := storage.LocalConfig{
			BasePath: cfg.Storage.Local.BasePath,
			Database: cfg.Storage.Local.Database,
		}
		return storage.NewLocalBackend(localCfg, logger)
	case "minio":
		minioCfg := storage.MinIOConfig{
			Endpoint:  cfg.Storage.MinIO.Endpoint,
			AccessKey: cfg.Storage.MinIO.AccessKey,
			SecretKey: cfg.Storage.MinIO.SecretKey,
			Bucket:    cfg.Storage.MinIO.Bucket,
			Database:  cfg.Storage.MinIO.Database,
			UseSSL:    cfg.Storage.MinIO.UseSSL,
		}
		return storage.NewMinIOBackend(minioCfg, logger)
	case "s3":
		s3Cfg := storage.S3Config{
			AccessKey: cfg.Storage.S3.AccessKey,
			SecretKey: cfg.Storage.S3.SecretKey,
			Region:    cfg.Storage.S3.Region,
			Bucket:    cfg.Storage.S3.Bucket,
			Database:  cfg.Storage.S3.Database,
		}
		return storage.NewS3Backend(s3Cfg, logger)
	case "gcs":
		gcsCfg := storage.GCSConfig{
			Bucket:          cfg.Storage.GCS.Bucket,
			ProjectID:       cfg.Storage.GCS.ProjectID,
			CredentialsFile: cfg.Storage.GCS.CredentialsFile,
			Database:        cfg.Storage.GCS.Database,
		}
		return storage.NewGCSBackend(gcsCfg, logger)
	default:
		return nil, fmt.Errorf("unsupported storage backend: %s", cfg.Storage.Backend)
	}
}

// initializeServer creates and configures the HTTP server
func initializeServer(cfg *config.Config, backend storage.Backend, walInstance *wal.WAL, monitor *monitoring.Monitor, logger *logrus.Logger) *http.Server {
	// Create Gin router
	router := gin.New()

	// Add middleware
	router.Use(gin.LoggerWithWriter(io.Writer(logger.Writer())))
	router.Use(gin.Recovery())

	// Add CORS middleware
	if len(cfg.GetCORSOrigins()) > 0 {
		corsConfig := cors.DefaultConfig()
		corsConfig.AllowOrigins = cfg.GetCORSOrigins()
		corsConfig.AllowMethods = []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"}
		corsConfig.AllowHeaders = []string{"Origin", "Content-Type", "Authorization"}
		router.Use(cors.New(corsConfig))
	}

	// Add authentication middleware if enabled
	if cfg.IsAuthEnabled() {
		authMiddleware := auth.NewMiddleware(cfg.Auth.DefaultToken, cfg.GetAuthAllowlist(), logger)
		router.Use(authMiddleware.Middleware())
	}

	// Create query engine for shared use across handlers
	pathBuilder := storage.NewDefaultPathBuilder()
	duckdbConfig := query.DuckDBConfig{
		PoolSize:          cfg.DuckDB.PoolSize,
		MaxQueueSize:      cfg.DuckDB.MaxQueueSize,
		MemoryLimit:       cfg.DuckDB.MemoryLimit,
		Threads:           cfg.DuckDB.Threads,
		TempDirectory:     cfg.DuckDB.TempDirectory,
		EnableObjectCache: cfg.DuckDB.EnableObjectCache,
	}

	queryEngine, err := query.NewDuckDBEngine(duckdbConfig, backend, pathBuilder, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to create DuckDB query engine")
	}

	// Register routes
	registerRoutes(router, cfg, backend, walInstance, queryEngine, monitor, logger)

	// Create HTTP server
	server := &http.Server{
		Addr:         cfg.GetListenAddress(),
		Handler:      router,
		ReadTimeout:  time.Duration(cfg.Server.WorkerTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.Server.WorkerTimeout) * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return server
}

// initializeDatabaseManager creates and initializes the database manager
func initializeDatabaseManager(cfg *config.Config, logger *logrus.Logger) (*database.Manager, error) {
	// Use fixed database path
	dbPath := "./data/arc.db"

	dbMgr, err := database.NewManager(dbPath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create database manager: %w", err)
	}

	logger.WithField("database_path", dbPath).Info("Database manager initialized")
	return dbMgr, nil
}

// registerRoutes registers all HTTP routes
func registerRoutes(router *gin.Engine, cfg *config.Config, backend storage.Backend, walInstance *wal.WAL, queryEngine *query.DuckDBEngine, monitor *monitoring.Monitor, logger *logrus.Logger) {
	// Health endpoints
	router.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})
	router.GET("/ready", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ready"})
	})

	// Initialize database manager (required for connections, jobs, schemas, etc.)
	dbMgr, err := initializeDatabaseManager(cfg, logger)
	if err != nil {
		logger.WithError(err).Fatal("Failed to initialize database manager")
	}

	// Create API handlers
	authHandler := v2.NewAuthHandler(cfg, logger)
	ingestionHandler := v2.NewIngestionHandler(cfg, backend, logger)
	queryHandler := v2.NewQueryHandlerWithEngine(cfg, backend, queryEngine, dbMgr, logger)
	exportHandler := v2.NewExportHandler(cfg, queryEngine, backend, logger)
	compactionHandler := v2.NewCompactionHandler(cfg, backend, logger)
	deleteHandler := v2.NewDeleteHandler(cfg, backend, logger)
	walHandler := v2.NewWALHandler(cfg, walInstance, logger)

	// New handlers for additional features
	connectionsHandler := v2.NewConnectionsHandler(cfg, dbMgr, logger)
	cacheHandler := v2.NewCacheHandler(cfg, queryHandler.GetCache(), logger)
	setupHandler := v2.NewSetupHandler(cfg, dbMgr, backend, logger)
	monitoringHandler := v2.NewMonitoringHandler(cfg, monitor, nil, logger)
	authAdvancedHandler := v2.NewAuthAdvancedHandler(cfg, dbMgr, logger)

	// API v1 routes
	apiV1 := router.Group("/api/v1")
	{
		// Monitoring endpoint
		apiV1.GET("/metrics", gin.WrapH(monitor.PrometheusHandler()))

		// Ingestion endpoints
		apiV1.POST("/write/msgpack", ingestionHandler.MessagePack)
		apiV1.POST("/write", ingestionHandler.LineProtocol)
		apiV1.POST("/write/influxdb2", ingestionHandler.InfluxDB2)
		apiV1.GET("/write/stats", ingestionHandler.GetStats)

		// Query endpoints
		apiV1.POST("/query", queryHandler.Query)
		apiV1.POST("/query/arrow", queryHandler.QueryArrow)
		apiV1.POST("/query/stream", queryHandler.QueryStream)
		apiV1.GET("/measurements/:database", queryHandler.Measurements)
		apiV1.POST("/explain", queryHandler.Explain)
		apiV1.GET("/query/health", queryHandler.Health)
		apiV1.GET("/query/metrics", queryHandler.GetMetrics)

		// Auth endpoints
		apiV1.POST("/auth/token", authHandler.CreateToken)
		apiV1.POST("/auth/verify", authHandler.VerifyToken)
		apiV1.POST("/auth/refresh", authHandler.RefreshToken)
		apiV1.POST("/auth/extend", authHandler.ExtendToken)
		apiV1.GET("/auth/info", authHandler.GetTokenInfo)
		apiV1.POST("/auth/logout", authHandler.Logout)

		// Export endpoints
		apiV1.POST("/export", exportHandler.Export)
		apiV1.GET("/export/:export_id/progress", exportHandler.GetExportProgress)
		apiV1.GET("/export/list", exportHandler.ListExports)
		apiV1.DELETE("/export/:export_id", exportHandler.CancelExport)
		apiV1.GET("/export/destinations", exportHandler.GetExportDestinations)
		apiV1.GET("/export/formats", exportHandler.GetExportFormats)
		apiV1.GET("/export/status", exportHandler.GetExportStatus)

		// Compaction endpoints
		apiV1.GET("/compaction/status", compactionHandler.GetStatus)
		apiV1.POST("/compaction/trigger", compactionHandler.TriggerCompaction)
		apiV1.POST("/compaction/run", compactionHandler.RunCompaction)
		apiV1.GET("/compaction/metrics", compactionHandler.GetMetrics)
		apiV1.GET("/compaction/locks", compactionHandler.GetLocks)
		apiV1.POST("/compaction/locks/cleanup", compactionHandler.CleanupLocks)
		apiV1.PUT("/compaction/config", compactionHandler.UpdateConfig)
		apiV1.POST("/compaction/start", compactionHandler.StartScheduler)
		apiV1.POST("/compaction/stop", compactionHandler.StopScheduler)
		apiV1.POST("/compaction/metrics/reset", compactionHandler.ResetMetrics)
		apiV1.GET("/compaction/health", compactionHandler.Health)

		// Delete endpoints
		apiV1.POST("/delete", deleteHandler.Delete)
		apiV1.DELETE("/database/:database", deleteHandler.DeleteDatabase)
		apiV1.DELETE("/database/:database/:measurement", deleteHandler.DeleteMeasurement)
		apiV1.GET("/delete/health", deleteHandler.Health)

		// Query estimation endpoint
		apiV1.POST("/query/estimate", queryHandler.EstimateQuery)

		// Query history endpoints
		apiV1.GET("/query/history", queryHandler.GetQueryHistory)
		apiV1.DELETE("/query/history/:id", queryHandler.DeleteQueryHistory)
		apiV1.DELETE("/query/history", queryHandler.ClearQueryHistory)

		// Setup and initialization endpoints
		apiV1.POST("/setup/default-connections", setupHandler.SetupDefaultConnections)
		apiV1.GET("/setup/ready", setupHandler.GetReadyStatus)
		apiV1.GET("/setup/info", setupHandler.GetSystemInfo)

		// Storage connections endpoints
		apiV1.GET("/connections/storage", connectionsHandler.GetStorageConnections)
		apiV1.POST("/connections/storage", connectionsHandler.AddStorageConnection)
		apiV1.PUT("/connections/storage/:id", connectionsHandler.UpdateStorageConnection)
		apiV1.DELETE("/connections/storage/:id", connectionsHandler.DeleteStorageConnection)
		apiV1.POST("/connections/storage/:id/activate", connectionsHandler.ActivateStorageConnection)
		apiV1.POST("/connections/storage/test", connectionsHandler.TestStorageConnection)

		// InfluxDB connections endpoints
		apiV1.GET("/connections/influx", connectionsHandler.GetInfluxConnections)
		apiV1.POST("/connections/influx", connectionsHandler.AddInfluxConnection)
		apiV1.PUT("/connections/influx/:id", connectionsHandler.UpdateInfluxConnection)
		apiV1.DELETE("/connections/influx/:id", connectionsHandler.DeleteInfluxConnection)
		apiV1.POST("/connections/influx/:id/activate", connectionsHandler.ActivateInfluxConnection)

		// HTTP JSON connections endpoints
		apiV1.GET("/connections/http-json", connectionsHandler.GetHTTPJSONConnections)
		apiV1.POST("/connections/http-json", connectionsHandler.AddHTTPJSONConnection)
		apiV1.PUT("/connections/http-json/:id", connectionsHandler.UpdateHTTPJSONConnection)
		apiV1.DELETE("/connections/http-json/:id", connectionsHandler.DeleteHTTPJSONConnection)

		// Cache management endpoints
		apiV1.GET("/cache/stats", cacheHandler.GetStats)
		apiV1.GET("/cache/health", cacheHandler.GetHealth)
		apiV1.POST("/cache/clear", cacheHandler.Clear)
		apiV1.POST("/cache/invalidate", cacheHandler.InvalidateDatabase)

		// Advanced monitoring endpoints
		apiV1.GET("/monitoring/logs", monitoringHandler.GetLogs)
		apiV1.GET("/monitoring/logs/stats", monitoringHandler.GetLogStats)
		apiV1.GET("/monitoring/endpoints", monitoringHandler.GetEndpointStats)
		apiV1.GET("/monitoring/memory", monitoringHandler.GetMemoryStats)
		apiV1.GET("/monitoring/metrics/:type", monitoringHandler.GetTimeSeriesMetrics)
		apiV1.GET("/monitoring/health", monitoringHandler.GetSystemHealth)

		// Advanced auth endpoints
		apiV1.GET("/auth/tokens", authAdvancedHandler.ListAPITokens)
		apiV1.POST("/auth/tokens", authAdvancedHandler.CreateAPIToken)
		apiV1.GET("/auth/tokens/:id", authAdvancedHandler.GetAPIToken)
		apiV1.PUT("/auth/tokens/:id", authAdvancedHandler.UpdateAPIToken)
		apiV1.DELETE("/auth/tokens/:id", authAdvancedHandler.DeleteAPIToken)
		apiV1.POST("/auth/tokens/:id/rotate", authAdvancedHandler.RotateAPIToken)
		apiV1.GET("/auth/cache/stats", authAdvancedHandler.GetAuthCacheStats)
		apiV1.POST("/auth/cache/invalidate", authAdvancedHandler.InvalidateAuthCache)

		// WAL management endpoints
		apiV1.GET("/wal/status", walHandler.GetStatus)
		apiV1.GET("/wal/stats", walHandler.GetStats)
		apiV1.GET("/wal/files", walHandler.ListFiles)
		apiV1.POST("/wal/recover", walHandler.RecoverFromWAL)
		apiV1.POST("/wal/rotate", walHandler.RotateWAL)
	}

	// Static file serving for SQL query interface
	router.GET("/sql", func(c *gin.Context) {
		c.File("./assets/sql-query.html")
	})

	router.GET("/query-ui", func(c *gin.Context) {
		c.File("./assets/sql-query.html")
	})

	// Root endpoint
	router.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"name":        AppName,
			"version":     AppVersion,
			"description": AppDesc,
			"status":      "running",
			"endpoints": gin.H{
				"health":   "/health",
				"ready":    "/ready",
				"api":      "/api/v1",
				"metrics":  "/api/v1/metrics",
				"write":    "/api/v1/write",
				"query":    "/api/v1/query",
				"auth":     "/api/v1/auth",
				"export":   "/api/v1/export",
				"sql_ui":   "/sql",
				"query_ui": "/query-ui",
			},
		})
	})
}

// waitForShutdown waits for shutdown signal and gracefully shuts down the server
func waitForShutdown(server *http.Server, cancel context.CancelFunc, logger *logrus.Logger) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	logger.Info("Shutting down server...")

	// Cancel context
	cancel()

	// Create shutdown context with timeout
	ctx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := server.Shutdown(ctx); err != nil {
		logger.WithError(err).Error("Server forced to shutdown")
	} else {
		logger.Info("Server shutdown complete")
	}
}

// showHelp displays help information
func showHelp() {
	fmt.Printf(`%s - %s

Usage:
  %s [options]

Options:
  -config string
        Path to configuration file (default "arc.conf")
  -version
        Show version information
  -help
        Show this help message

Examples:
  %s                           # Start with default config
  %s -config prod.conf         # Start with production config
  %s -version                  # Show version

Configuration:
  Configuration is loaded from a TOML file. Environment variables override
  file settings with ARC_ prefix (e.g., ARC_SERVER_PORT=8080).

  See arc.conf for detailed configuration options.

`, AppName, AppDesc, os.Args[0], os.Args[0], os.Args[0], os.Args[0])
}

// showVersion displays version information
func showVersion() {
	fmt.Printf(`%s %s

Build:
  Go version: %s
  OS/Arch:    %s/%s
  Built:       %s

%s
`, AppName, AppVersion, runtime.Version(), runtime.GOOS, runtime.GOARCH, buildTime(), AppDesc)
}

// buildTime returns the build time (would be set by build flags)
func buildTime() string {
	// In a real build, this would be set via ldflags
	// -ldflags "-X main.buildTime=$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
	return "development"
}

// walRecoveryHandler implements the WAL recovery handler interface
type walRecoveryHandler struct {
	storage     storage.Backend
	pathBuilder storage.PathBuilder
	logger      *logrus.Logger
}

// RecoverEntry recovers a single WAL entry by writing it back to storage
func (h *walRecoveryHandler) RecoverEntry(ctx context.Context, entry wal.WALEntry) error {
	h.logger.WithFields(logrus.Fields{
		"sequence":    entry.Sequence,
		"database":    entry.Database,
		"measurement": entry.Measurement,
		"data_size":   len(entry.Data),
	}).Debug("Recovering WAL entry")

	// For now, we'll just log the entry
	// In a production system, you would:
	// 1. Parse the data based on content-type header
	// 2. Convert to Arrow record
	// 3. Write to Parquet storage
	// This would require access to the ingestion pipeline

	h.logger.WithFields(logrus.Fields{
		"sequence":    entry.Sequence,
		"database":    entry.Database,
		"measurement": entry.Measurement,
	}).Info("WAL entry recovered (logged only - full recovery requires ingestion pipeline)")

	return nil
}

// generateOpenAPI generates OpenAPI specification
func generateOpenAPI(cfg *config.Config) map[string]interface{} {
	return map[string]interface{}{
		"openapi": "3.0.0",
		"info": map[string]interface{}{
			"title":       "Arc Core API",
			"description": "High-performance time-series data warehouse API",
			"version":     AppVersion,
		},
		"servers": []map[string]interface{}{
			{
				"url":         fmt.Sprintf("http://%s:%d", cfg.Server.Host, cfg.Server.Port),
				"description": "Arc Core Server",
			},
		},
		"paths": map[string]interface{}{
			"/health": map[string]interface{}{
				"get": map[string]interface{}{
					"summary":     "Health check",
					"description": "Returns the health status of the server",
					"responses": map[string]interface{}{
						"200": map[string]interface{}{
							"description": "Server is healthy",
							"content": map[string]interface{}{
								"application/json": map[string]interface{}{
									"schema": map[string]interface{}{
										"type": "object",
										"properties": map[string]interface{}{
											"status": map[string]interface{}{
												"type": "string",
											},
											"timestamp": map[string]interface{}{
												"type": "string",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
