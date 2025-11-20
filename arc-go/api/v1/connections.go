package v1

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// ConnectionsHandler handles connection management endpoints
type ConnectionsHandler struct {
	config *config.Config
	dbMgr  *database.Manager
	logger *logrus.Logger
}

// NewConnectionsHandler creates a new connections handler
func NewConnectionsHandler(cfg *config.Config, dbMgr *database.Manager, logger *logrus.Logger) *ConnectionsHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &ConnectionsHandler{
		config: cfg,
		dbMgr:  dbMgr,
		logger: logger,
	}
}

// ============================================================================
// InfluxDB Connections
// ============================================================================

// GetInfluxConnections returns all InfluxDB connections
func (h *ConnectionsHandler) GetInfluxConnections(c *gin.Context) {
	connections, err := h.dbMgr.GetInfluxConnections()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get InfluxDB connections")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Mask sensitive fields
	for _, conn := range connections {
		if conn.Password != nil {
			masked := "***"
			conn.Password = &masked
		}
		if conn.Token != nil {
			masked := "***"
			conn.Token = &masked
		}
	}

	c.JSON(http.StatusOK, connections)
}

// AddInfluxConnection adds a new InfluxDB connection
func (h *ConnectionsHandler) AddInfluxConnection(c *gin.Context) {
	var conn database.InfluxConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id, err := h.dbMgr.AddInfluxConnection(&conn)
	if err != nil {
		h.logger.WithError(err).Error("Failed to add InfluxDB connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      id,
		"message": "InfluxDB connection added successfully",
	})
}

// UpdateInfluxConnection updates an InfluxDB connection
func (h *ConnectionsHandler) UpdateInfluxConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	var conn database.InfluxConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.dbMgr.UpdateInfluxConnection(id, &conn); err != nil {
		h.logger.WithError(err).Error("Failed to update InfluxDB connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "InfluxDB connection updated successfully"})
}

// DeleteInfluxConnection deletes an InfluxDB connection
func (h *ConnectionsHandler) DeleteInfluxConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	if err := h.dbMgr.DeleteInfluxConnection(id); err != nil {
		h.logger.WithError(err).Error("Failed to delete InfluxDB connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "InfluxDB connection deleted successfully"})
}

// ActivateInfluxConnection sets an InfluxDB connection as active
func (h *ConnectionsHandler) ActivateInfluxConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	if err := h.dbMgr.SetActiveInfluxConnection(id); err != nil {
		h.logger.WithError(err).Error("Failed to activate InfluxDB connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "InfluxDB connection activated successfully"})
}

// ============================================================================
// Storage Connections
// ============================================================================

// GetStorageConnections returns all storage connections
func (h *ConnectionsHandler) GetStorageConnections(c *gin.Context) {
	connections, err := h.dbMgr.GetStorageConnections()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get storage connections")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Mask sensitive fields
	for _, conn := range connections {
		if conn.AccessKey != nil {
			masked := "***"
			conn.AccessKey = &masked
		}
		if conn.SecretKey != nil {
			masked := "***"
			conn.SecretKey = &masked
		}
		if conn.HMACSecret != nil {
			masked := "***"
			conn.HMACSecret = &masked
		}
		if conn.CredentialsJSON != nil {
			masked := "***"
			conn.CredentialsJSON = &masked
		}
	}

	c.JSON(http.StatusOK, connections)
}

// AddStorageConnection adds a new storage connection
func (h *ConnectionsHandler) AddStorageConnection(c *gin.Context) {
	var conn database.StorageConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id, err := h.dbMgr.AddStorageConnection(&conn)
	if err != nil {
		h.logger.WithError(err).Error("Failed to add storage connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      id,
		"message": "Storage connection added successfully",
	})
}

// UpdateStorageConnection updates a storage connection
func (h *ConnectionsHandler) UpdateStorageConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	var conn database.StorageConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.dbMgr.UpdateStorageConnection(id, &conn); err != nil {
		h.logger.WithError(err).Error("Failed to update storage connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Storage connection updated successfully"})
}

// DeleteStorageConnection deletes a storage connection
func (h *ConnectionsHandler) DeleteStorageConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	if err := h.dbMgr.DeleteStorageConnection(id); err != nil {
		h.logger.WithError(err).Error("Failed to delete storage connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Storage connection deleted successfully"})
}

// ActivateStorageConnection sets a storage connection as active
func (h *ConnectionsHandler) ActivateStorageConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	if err := h.dbMgr.SetActiveStorageConnection(id); err != nil {
		h.logger.WithError(err).Error("Failed to activate storage connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Storage connection activated successfully"})
}

// TestStorageConnection tests a storage connection
func (h *ConnectionsHandler) TestStorageConnection(c *gin.Context) {
	var conn database.StorageConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create temporary backend based on connection type
	var backend storage.Backend
	var err error

	switch conn.Backend {
	case "local":
		basePath := "./data/arc"
		if conn.BasePath != nil {
			basePath = *conn.BasePath
		}
		cfg := storage.LocalConfig{
			BasePath: basePath,
			Database: conn.Database,
		}
		backend, err = storage.NewLocalBackend(cfg, h.logger)

	case "minio":
		if conn.Endpoint == nil || conn.AccessKey == nil || conn.SecretKey == nil || conn.Bucket == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required MinIO connection parameters"})
			return
		}
		cfg := storage.MinIOConfig{
			Endpoint:  *conn.Endpoint,
			AccessKey: *conn.AccessKey,
			SecretKey: *conn.SecretKey,
			Bucket:    *conn.Bucket,
			Database:  conn.Database,
			UseSSL:    conn.SSL,
		}
		backend, err = storage.NewMinIOBackend(cfg, h.logger)

	case "s3":
		if conn.AccessKey == nil || conn.SecretKey == nil || conn.Bucket == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required S3 connection parameters"})
			return
		}
		cfg := storage.S3Config{
			AccessKey: *conn.AccessKey,
			SecretKey: *conn.SecretKey,
			Region:    conn.Region,
			Bucket:    *conn.Bucket,
			Database:  conn.Database,
		}
		backend, err = storage.NewS3Backend(cfg, h.logger)

	case "gcs":
		if conn.Bucket == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Missing required GCS bucket"})
			return
		}
		cfg := storage.GCSConfig{
			Bucket:          *conn.Bucket,
			ProjectID:       stringValue(conn.ProjectID),
			CredentialsFile: stringValue(conn.CredentialsFile),
			Database:        conn.Database,
		}
		backend, err = storage.NewGCSBackend(cfg, h.logger)

	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("Unsupported backend: %s", conn.Backend)})
		return
	}

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "Failed to create backend",
			"details": err.Error(),
		})
		return
	}
	defer backend.Close()

	// Test connection with List operation
	ctx := c.Request.Context()
	_, err = backend.List(ctx, "")
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"success": false,
			"message": "Connection test failed",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": fmt.Sprintf("%s connection test successful", conn.Backend),
	})
}

// ============================================================================
// HTTP JSON Connections
// ============================================================================

// GetHTTPJSONConnections returns all HTTP JSON connections
func (h *ConnectionsHandler) GetHTTPJSONConnections(c *gin.Context) {
	connections, err := h.dbMgr.GetHTTPJSONConnections()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get HTTP JSON connections")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Mask sensitive auth config
	for _, conn := range connections {
		if conn.AuthConfig != nil {
			masked := "***"
			conn.AuthConfig = &masked
		}
	}

	c.JSON(http.StatusOK, connections)
}

// AddHTTPJSONConnection adds a new HTTP JSON connection
func (h *ConnectionsHandler) AddHTTPJSONConnection(c *gin.Context) {
	var conn database.HTTPJSONConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id, err := h.dbMgr.AddHTTPJSONConnection(&conn)
	if err != nil {
		h.logger.WithError(err).Error("Failed to add HTTP JSON connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":      id,
		"message": "HTTP JSON connection added successfully",
	})
}

// UpdateHTTPJSONConnection updates an HTTP JSON connection
func (h *ConnectionsHandler) UpdateHTTPJSONConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	var conn database.HTTPJSONConnection
	if err := c.ShouldBindJSON(&conn); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.dbMgr.UpdateHTTPJSONConnection(id, &conn); err != nil {
		h.logger.WithError(err).Error("Failed to update HTTP JSON connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "HTTP JSON connection updated successfully"})
}

// DeleteHTTPJSONConnection deletes an HTTP JSON connection
func (h *ConnectionsHandler) DeleteHTTPJSONConnection(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid connection ID"})
		return
	}

	if err := h.dbMgr.DeleteHTTPJSONConnection(id); err != nil {
		h.logger.WithError(err).Error("Failed to delete HTTP JSON connection")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "HTTP JSON connection deleted successfully"})
}

// Helper function to get string value from pointer
func stringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
