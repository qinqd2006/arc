package v1

import (
	"net/http"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// SetupHandler handles setup and initialization endpoints
type SetupHandler struct {
	config  *config.Config
	dbMgr   *database.Manager
	storage storage.Backend
	logger  *logrus.Logger
}

// NewSetupHandler creates a new setup handler
func NewSetupHandler(cfg *config.Config, dbMgr *database.Manager, backend storage.Backend, logger *logrus.Logger) *SetupHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &SetupHandler{
		config:  cfg,
		dbMgr:   dbMgr,
		storage: backend,
		logger:  logger,
	}
}

// SetupDefaultConnections creates default connections based on current config
func (h *SetupHandler) SetupDefaultConnections(c *gin.Context) {
	var createdConnections []map[string]interface{}
	var errors []string

	// Create storage connection based on current config
	var storageConn *database.StorageConnection

	switch h.config.Storage.Backend {
	case "local":
		storageConn = &database.StorageConnection{
			Name:     "Default Local Storage",
			Backend:  "local",
			BasePath: stringPtrSetup(h.config.Storage.Local.BasePath),
			Database: h.config.Storage.Local.Database,
			IsActive: true,
		}
	case "minio":
		storageConn = &database.StorageConnection{
			Name:      "Default MinIO",
			Backend:   "minio",
			Endpoint:  stringPtrSetup(h.config.Storage.MinIO.Endpoint),
			AccessKey: stringPtrSetup(h.config.Storage.MinIO.AccessKey),
			SecretKey: stringPtrSetup(h.config.Storage.MinIO.SecretKey),
			Bucket:    stringPtrSetup(h.config.Storage.MinIO.Bucket),
			Database:  h.config.Storage.MinIO.Database,
			SSL:       h.config.Storage.MinIO.UseSSL,
			IsActive:  true,
		}
	case "s3":
		storageConn = &database.StorageConnection{
			Name:      "Default S3",
			Backend:   "s3",
			AccessKey: stringPtrSetup(h.config.Storage.S3.AccessKey),
			SecretKey: stringPtrSetup(h.config.Storage.S3.SecretKey),
			Bucket:    stringPtrSetup(h.config.Storage.S3.Bucket),
			Database:  h.config.Storage.S3.Database,
			Region:    h.config.Storage.S3.Region,
			IsActive:  true,
		}
	case "gcs":
		storageConn = &database.StorageConnection{
			Name:            "Default GCS",
			Backend:         "gcs",
			Bucket:          stringPtrSetup(h.config.Storage.GCS.Bucket),
			Database:        h.config.Storage.GCS.Database,
			ProjectID:       stringPtrSetup(h.config.Storage.GCS.ProjectID),
			CredentialsFile: stringPtrSetup(h.config.Storage.GCS.CredentialsFile),
			IsActive:        true,
		}
	}

	if storageConn != nil {
		id, err := h.dbMgr.AddStorageConnection(storageConn)
		if err != nil {
			errors = append(errors, "Failed to create storage connection: "+err.Error())
		} else {
			createdConnections = append(createdConnections, map[string]interface{}{
				"type": "storage",
				"id":   id,
				"name": storageConn.Name,
			})
			h.logger.WithFields(logrus.Fields{
				"id":      id,
				"backend": storageConn.Backend,
			}).Info("Default storage connection created")
		}
	}

	// Check if we have any errors
	if len(errors) > 0 {
		c.JSON(http.StatusPartialContent, gin.H{
			"message":             "Default connections partially created",
			"created_connections": createdConnections,
			"errors":              errors,
		})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"message":             "Default connections created successfully",
		"created_connections": createdConnections,
	})
}

// GetReadyStatus returns comprehensive readiness check with component status
func (h *SetupHandler) GetReadyStatus(c *gin.Context) {
	components := make(map[string]interface{})
	overallReady := true

	// Check storage backend
	storageHealthy := false
	if h.storage != nil {
		// Try to list files as a health check
		ctx := c.Request.Context()
		_, err := h.storage.List(ctx, "")
		if err != nil {
			components["storage"] = map[string]interface{}{
				"ready":   false,
				"message": err.Error(),
			}
			overallReady = false
		} else {
			components["storage"] = map[string]interface{}{
				"ready":   true,
				"backend": h.config.Storage.Backend,
			}
			storageHealthy = true
		}
	} else {
		components["storage"] = map[string]interface{}{
			"ready":   false,
			"message": "Storage backend not initialized",
		}
		overallReady = false
	}

	// Check database connections
	dbHealthy := false
	if h.dbMgr != nil {
		storageConns, err := h.dbMgr.GetStorageConnections()
		if err != nil {
			components["database"] = map[string]interface{}{
				"ready":   false,
				"message": "Failed to query database: " + err.Error(),
			}
			overallReady = false
		} else {
			components["database"] = map[string]interface{}{
				"ready":              true,
				"storage_configured": len(storageConns),
			}
			dbHealthy = true
		}
	} else {
		components["database"] = map[string]interface{}{
			"ready":   false,
			"message": "Database manager not initialized",
		}
		overallReady = false
	}

	// Configuration check
	components["configuration"] = map[string]interface{}{
		"ready":         true,
		"server_port":   h.config.Server.Port,
		"auth_enabled":  h.config.IsAuthEnabled(),
		"cache_enabled": h.config.QueryCache.Enabled,
	}

	// Determine HTTP status code
	statusCode := http.StatusOK
	if !overallReady {
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, gin.H{
		"ready":      overallReady,
		"components": components,
		"checks": map[string]bool{
			"storage_healthy":  storageHealthy,
			"database_healthy": dbHealthy,
		},
	})
}

// GetSystemInfo returns system information and capabilities
func (h *SetupHandler) GetSystemInfo(c *gin.Context) {
	// Get connection counts
	storageConns, _ := h.dbMgr.GetStorageConnections()
	influxConns, _ := h.dbMgr.GetInfluxConnections()
	httpJSONConns, _ := h.dbMgr.GetHTTPJSONConnections()
	exportJobs, _ := h.dbMgr.GetExportJobs()
	avroSchemas, _ := h.dbMgr.GetAvroSchemas()

	c.JSON(http.StatusOK, gin.H{
		"version": "1.0.0",
		"storage": gin.H{
			"backend":  h.config.Storage.Backend,
			"database": getStorageDatabase(h.config),
		},
		"features": gin.H{
			"authentication": h.config.IsAuthEnabled(),
			"query_cache":    h.config.QueryCache.Enabled,
			"wal_enabled":    h.config.WAL.Enabled,
			"compaction":     h.config.Compaction.Enabled,
			"monitoring":     h.config.Monitoring.Enabled,
			"job_scheduling": true,
			"avro_schemas":   true,
			"rate_limiting":  true,
		},
		"connections": gin.H{
			"storage_backends":   len(storageConns),
			"influx_datasources": len(influxConns),
			"http_json_sources":  len(httpJSONConns),
		},
		"automation": gin.H{
			"export_jobs":  len(exportJobs),
			"avro_schemas": len(avroSchemas),
		},
		"api": gin.H{
			"version": "v1",
			"endpoints": []string{
				"/api/v1/write",
				"/api/v1/query",
				"/api/v1/connections",
				"/api/v1/jobs",
				"/api/v1/cache",
				"/api/v1/avro/schemas",
			},
		},
	})
}

// Helper functions

func stringPtrSetup(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func getStorageDatabase(cfg *config.Config) string {
	switch cfg.Storage.Backend {
	case "local":
		return cfg.Storage.Local.Database
	case "minio":
		return cfg.Storage.MinIO.Database
	case "s3":
		return cfg.Storage.S3.Database
	case "gcs":
		return cfg.Storage.GCS.Database
	default:
		return "default"
	}
}
