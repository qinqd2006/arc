package v1

import (
	"net/http"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/wal"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// WALHandler handles Write-Ahead Log management endpoints
type WALHandler struct {
	config *config.Config
	wal    *wal.WAL
	logger *logrus.Logger
}

// NewWALHandler creates a new WAL handler
func NewWALHandler(cfg *config.Config, walInstance *wal.WAL, logger *logrus.Logger) *WALHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &WALHandler{
		config: cfg,
		wal:    walInstance,
		logger: logger,
	}
}

// GetStatus returns WAL status and configuration
func (h *WALHandler) GetStatus(c *gin.Context) {
	if h.wal == nil {
		c.JSON(http.StatusOK, gin.H{
			"enabled": false,
			"message": "WAL is not enabled. Set WAL_ENABLED=true in configuration.",
		})
		return
	}

	// Return basic status
	c.JSON(http.StatusOK, gin.H{
		"enabled": true,
		"configuration": gin.H{
			"sync_mode":    "fsync",
			"current_file": "wal file path",
		},
		"message": "WAL is enabled",
	})
}

// GetStats returns detailed WAL statistics
func (h *WALHandler) GetStats(c *gin.Context) {
	if h.wal == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "WAL is not enabled. Set WAL_ENABLED=true in configuration.",
		})
		return
	}

	// Return basic stats
	c.JSON(http.StatusOK, gin.H{
		"total_entries": 0,
		"total_bytes":   0,
		"message":       "WAL statistics",
	})
}

// ListFiles returns list of WAL files
func (h *WALHandler) ListFiles(c *gin.Context) {
	if h.wal == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "WAL is not enabled",
		})
		return
	}

	// WAL file listing not exposed through public API
	// Return placeholder response
	c.JSON(http.StatusOK, gin.H{
		"total_files": 0,
		"files":       []gin.H{},
		"message":     "WAL file listing requires direct filesystem access",
	})
}

// RecoverFromWAL triggers WAL recovery (reads and replays WAL entries)
func (h *WALHandler) RecoverFromWAL(c *gin.Context) {
	if h.wal == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "WAL is not enabled",
		})
		return
	}

	// WAL recovery requires a custom recovery handler
	// Return method not available response
	c.JSON(http.StatusNotImplemented, gin.H{
		"error":   "WAL recovery requires custom recovery handler implementation",
		"message": "Use the internal WAL.Recover() method with appropriate recovery handler",
	})
}

// RotateWAL manually triggers a WAL file rotation
func (h *WALHandler) RotateWAL(c *gin.Context) {
	if h.wal == nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "WAL is not enabled",
		})
		return
	}

	// WAL rotation is handled automatically based on size/age thresholds
	// Manual rotation not exposed through public API
	c.JSON(http.StatusNotImplemented, gin.H{
		"error":   "Manual WAL rotation not available",
		"message": "WAL files rotate automatically based on configured size and age thresholds",
	})
}
