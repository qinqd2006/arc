package v1

import (
	"context"
	"net/http"
	"time"

	"github.com/arc-core/arc-go/internal/compaction"
	"github.com/arc-core/arc-go/internal/config"
	storagelib "github.com/arc-core/arc-go/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// CompactionHandler handles compaction API endpoints
type CompactionHandler struct {
	config    *config.Config
	storage   storagelib.Backend
	compactor *compaction.Compactor
	scheduler *compaction.Scheduler
	logger    *logrus.Logger
}

// NewCompactionHandler creates a new compaction handler
func NewCompactionHandler(
	cfg *config.Config,
	storage storagelib.Backend,
	logger *logrus.Logger,
) *CompactionHandler {
	if logger == nil {
		logger = logrus.New()
	}

	pathBuilder := storagelib.NewDefaultPathBuilder()

	// Create compactor
	compactor := compaction.NewCompactor(&cfg.Compaction, storage, pathBuilder, logger)

	// Create scheduler
	scheduler := compaction.NewScheduler(&cfg.Compaction, storage, compactor, logger)

	// Start scheduler if enabled
	if cfg.Compaction.Enabled {
		if err := scheduler.Start(); err != nil {
			logger.WithError(err).Error("Failed to start compaction scheduler")
		}
	}

	return &CompactionHandler{
		config:    cfg,
		storage:   storage,
		compactor: compactor,
		scheduler: scheduler,
		logger:    logger,
	}
}

// GetStatus returns the compaction scheduler status
func (h *CompactionHandler) GetStatus(c *gin.Context) {
	status := h.scheduler.GetStatus()

	c.JSON(http.StatusOK, gin.H{
		"status":     "success",
		"compaction": status,
	})
}

// TriggerCompaction triggers an immediate compaction run
func (h *CompactionHandler) TriggerCompaction(c *gin.Context) {
	h.logger.Info("Manual compaction triggered via API")

	if err := h.scheduler.Trigger(); err != nil {
		h.logger.WithError(err).Error("Failed to trigger compaction")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to trigger compaction",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"status":  "accepted",
		"message": "Compaction job has been triggered",
	})
}

// RunCompaction runs compaction synchronously and returns the result
func (h *CompactionHandler) RunCompaction(c *gin.Context) {
	h.logger.Info("Synchronous compaction triggered via API")

	start := time.Now()

	// Run compaction
	ctx := context.Background()
	result, err := h.compactor.Compact(ctx)
	if err != nil {
		h.logger.WithError(err).Error("Compaction failed")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Compaction failed",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":               "success",
		"files_compacted":      result.FilesCompacted,
		"files_created":        result.FilesCreated,
		"bytes_processed":      result.BytesProcessed,
		"bytes_written":        result.BytesWritten,
		"partitions_compacted": result.PartitionsCompacted,
		"duration":             time.Since(start).String(),
	})
}

// GetMetrics returns compaction metrics
func (h *CompactionHandler) GetMetrics(c *gin.Context) {
	metrics := h.scheduler.GetMetrics()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"metrics": metrics,
	})
}

// GetLocks returns active compaction locks
func (h *CompactionHandler) GetLocks(c *gin.Context) {
	locks := h.compactor.GetLocks()

	c.JSON(http.StatusOK, gin.H{
		"status": "success",
		"locks":  locks,
		"count":  len(locks),
	})
}

// CleanupLocks removes expired compaction locks
func (h *CompactionHandler) CleanupLocks(c *gin.Context) {
	h.compactor.CleanupExpiredLocks()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Expired locks cleaned up",
	})
}

// UpdateConfig updates compaction configuration
func (h *CompactionHandler) UpdateConfig(c *gin.Context) {
	var req struct {
		Enabled           bool   `json:"enabled"`
		Schedule          string `json:"schedule,omitempty"`
		MinFiles          int    `json:"min_files,omitempty"`
		MinAgeHours       int    `json:"min_age_hours,omitempty"`
		TargetFileSizeMB  int    `json:"target_file_size_mb,omitempty"`
		MaxConcurrentJobs int    `json:"max_concurrent_jobs,omitempty"`
		LockTTLHours      int    `json:"lock_ttl_hours,omitempty"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request body",
			"details": err.Error(),
		})
		return
	}

	// Update config
	newConfig := h.config.Compaction

	if req.Schedule != "" {
		newConfig.Schedule = req.Schedule
	}
	if req.MinFiles > 0 {
		newConfig.MinFiles = req.MinFiles
	}
	if req.MinAgeHours > 0 {
		newConfig.MinAgeHours = req.MinAgeHours
	}
	if req.TargetFileSizeMB > 0 {
		newConfig.TargetFileSizeMB = req.TargetFileSizeMB
	}
	if req.MaxConcurrentJobs > 0 {
		newConfig.MaxConcurrentJobs = req.MaxConcurrentJobs
	}
	if req.LockTTLHours > 0 {
		newConfig.LockTTLHours = req.LockTTLHours
	}
	newConfig.Enabled = req.Enabled

	if err := h.scheduler.UpdateConfig(&newConfig); err != nil {
		h.logger.WithError(err).Error("Failed to update compaction config")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to update configuration",
			"details": err.Error(),
		})
		return
	}

	h.config.Compaction = newConfig

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Configuration updated successfully",
		"config":  newConfig,
	})
}

// StartScheduler starts the compaction scheduler
func (h *CompactionHandler) StartScheduler(c *gin.Context) {
	if err := h.scheduler.Start(); err != nil {
		h.logger.WithError(err).Error("Failed to start scheduler")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to start scheduler",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Scheduler started successfully",
	})
}

// StopScheduler stops the compaction scheduler
func (h *CompactionHandler) StopScheduler(c *gin.Context) {
	if err := h.scheduler.Stop(); err != nil {
		h.logger.WithError(err).Error("Failed to stop scheduler")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to stop scheduler",
			"details": err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Scheduler stopped successfully",
	})
}

// ResetMetrics resets compaction metrics
func (h *CompactionHandler) ResetMetrics(c *gin.Context) {
	h.scheduler.ResetMetrics()

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Metrics reset successfully",
	})
}

// Health checks compaction health
func (h *CompactionHandler) Health(c *gin.Context) {
	if err := h.scheduler.Health(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "healthy",
	})
}

// Close gracefully shuts down the compaction handler
func (h *CompactionHandler) Close(ctx context.Context) error {
	h.logger.Info("CompactionHandler: Shutting down")

	if err := h.scheduler.Close(); err != nil {
		h.logger.WithError(err).Error("Failed to close scheduler")
	}

	h.logger.Info("CompactionHandler: Shutdown complete")
	return nil
}
