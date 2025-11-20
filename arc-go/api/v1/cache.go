package v1

import (
	"context"
	"net/http"

	"github.com/arc-core/arc-go/internal/cache"
	"github.com/arc-core/arc-go/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// CacheHandler handles cache management endpoints
type CacheHandler struct {
	config *config.Config
	cache  *cache.QueryCache
	logger *logrus.Logger
}

// NewCacheHandler creates a new cache handler
func NewCacheHandler(cfg *config.Config, queryCache *cache.QueryCache, logger *logrus.Logger) *CacheHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &CacheHandler{
		config: cfg,
		cache:  queryCache,
		logger: logger,
	}
}

// GetStats returns cache statistics
func (h *CacheHandler) GetStats(c *gin.Context) {
	metrics := h.cache.GetMetrics()

	c.JSON(http.StatusOK, gin.H{
		"enabled":      h.config.QueryCache.Enabled,
		"hits":         metrics.Hits,
		"misses":       metrics.Misses,
		"evictions":    metrics.Evictions,
		"entries":      metrics.Entries,
		"size_bytes":   metrics.SizeBytes,
		"hit_rate":     metrics.HitRate,
		"last_cleanup": metrics.LastCleanup,
		"ttl_seconds":  h.config.QueryCache.TTLSeconds,
		"max_size_mb":  h.config.QueryCache.MaxSizeMB,
		"max_entries":  h.config.QueryCache.MaxEntries,
	})
}

// GetHealth returns cache health status with warnings
func (h *CacheHandler) GetHealth(c *gin.Context) {
	if !h.config.QueryCache.Enabled {
		c.JSON(http.StatusOK, gin.H{
			"enabled": false,
			"healthy": true,
			"message": "Cache disabled",
		})
		return
	}

	metrics := h.cache.GetMetrics()
	warnings := []string{}
	healthy := true

	// Check hit rate
	if metrics.Hits+metrics.Misses > 100 && metrics.HitRate < 0.2 {
		warnings = append(warnings, "Low cache hit rate (<20%). Consider increasing TTL.")
		healthy = false
	}

	// Check utilization
	maxSize := int64(h.config.QueryCache.MaxSizeMB) * 1024 * 1024
	utilization := float64(metrics.SizeBytes) / float64(maxSize) * 100
	if metrics.Entries > 10 && utilization < 10 {
		warnings = append(warnings, "Cache underutilized (<10%). Consider reducing max_size.")
	}

	// Check evictions
	if metrics.Evictions > metrics.Hits/2 {
		warnings = append(warnings, "High eviction rate. Consider increasing max_size or TTL.")
		healthy = false
	}

	c.JSON(http.StatusOK, gin.H{
		"enabled":     true,
		"healthy":     healthy,
		"hit_rate":    metrics.HitRate,
		"utilization": utilization,
		"evictions":   metrics.Evictions,
		"warnings":    warnings,
	})
}

// Clear removes all cache entries
func (h *CacheHandler) Clear(c *gin.Context) {
	if !h.config.QueryCache.Enabled {
		c.JSON(http.StatusOK, gin.H{
			"message": "Query cache is disabled",
			"cleared": 0,
		})
		return
	}

	// Get current entry count before clearing
	metrics := h.cache.GetMetrics()
	count := metrics.Entries

	ctx := context.Background()
	if err := h.cache.Clear(ctx); err != nil {
		h.logger.WithError(err).Error("Failed to clear cache")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Cache cleared successfully",
		"cleared": count,
	})
}

// InvalidateDatabase removes all cache entries for a database
func (h *CacheHandler) InvalidateDatabase(c *gin.Context) {
	database := c.Param("database")
	if database == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Database name required"})
		return
	}

	if !h.config.QueryCache.Enabled {
		c.JSON(http.StatusOK, gin.H{
			"message": "Query cache is disabled",
		})
		return
	}

	ctx := context.Background()
	if err := h.cache.InvalidateDatabase(ctx, database); err != nil {
		h.logger.WithError(err).Error("Failed to invalidate database cache")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":  "Database cache invalidated successfully",
		"database": database,
	})
}
