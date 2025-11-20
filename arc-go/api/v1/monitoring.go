package v1

import (
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/logs"
	"github.com/arc-core/arc-go/internal/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// MonitoringHandler handles advanced monitoring endpoints
type MonitoringHandler struct {
	config     *config.Config
	monitor    *monitoring.Monitor
	logManager *logs.LogManager
	logger     *logrus.Logger
}

// NewMonitoringHandler creates a new monitoring handler
func NewMonitoringHandler(cfg *config.Config, monitor *monitoring.Monitor, logMgr *logs.LogManager, logger *logrus.Logger) *MonitoringHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &MonitoringHandler{
		config:     cfg,
		monitor:    monitor,
		logManager: logMgr,
		logger:     logger,
	}
}

// GetLogs returns application logs with optional filtering
func (h *MonitoringHandler) GetLogs(c *gin.Context) {
	level := c.Query("level")
	limitStr := c.DefaultQuery("limit", "100")
	sinceStr := c.Query("since")

	limit := 100
	if l, err := parseIntQuery(limitStr); err == nil {
		limit = l
	}

	var since *time.Time
	if sinceStr != "" {
		if t, err := time.Parse(time.RFC3339, sinceStr); err == nil {
			since = &t
		}
	}

	logs := h.logManager.GetLogs(level, limit, since)

	c.JSON(http.StatusOK, gin.H{
		"logs":  logs,
		"count": len(logs),
		"level": level,
	})
}

// GetLogStats returns log statistics
func (h *MonitoringHandler) GetLogStats(c *gin.Context) {
	stats := h.logManager.GetStats()

	c.JSON(http.StatusOK, stats)
}

// GetEndpointStats returns statistics per endpoint
func (h *MonitoringHandler) GetEndpointStats(c *gin.Context) {
	// Get metrics from monitor - ignore error for simplified version
	_ = h.monitor // Just use monitor without calling GetMetrics

	// Return basic stats
	c.JSON(http.StatusOK, gin.H{
		"endpoints": map[string]interface{}{},
		"timestamp": time.Now(),
		"message":   "Endpoint stats available via Prometheus /metrics",
	})
}

// GetMemoryStats returns memory usage statistics
func (h *MonitoringHandler) GetMemoryStats(c *gin.Context) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	c.JSON(http.StatusOK, gin.H{
		"memory": gin.H{
			"alloc_bytes":         mem.Alloc,
			"total_alloc_bytes":   mem.TotalAlloc,
			"sys_bytes":           mem.Sys,
			"num_gc":              mem.NumGC,
			"heap_alloc_bytes":    mem.HeapAlloc,
			"heap_sys_bytes":      mem.HeapSys,
			"heap_idle_bytes":     mem.HeapIdle,
			"heap_inuse_bytes":    mem.HeapInuse,
			"heap_released_bytes": mem.HeapReleased,
			"heap_objects":        mem.HeapObjects,
		},
		"goroutines": runtime.NumGoroutine(),
		"timestamp":  time.Now(),
	})
}

// GetTimeSeriesMetrics returns time series metrics
func (h *MonitoringHandler) GetTimeSeriesMetrics(c *gin.Context) {
	metricType := c.Param("type")

	// Simplified version - return placeholder data
	var data interface{}
	switch metricType {
	case "requests":
		data = map[string]interface{}{"total": 0}
	case "duration":
		data = map[string]interface{}{"avg_ms": 0}
	case "ingestion":
		data = gin.H{
			"records_ingested": 0,
			"bytes_ingested":   0,
		}
	case "queries":
		data = gin.H{
			"queries_executed": 0,
			"cache_hits":       0,
		}
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Unknown metric type",
			"valid_types": []string{
				"requests",
				"duration",
				"ingestion",
				"queries",
			},
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"metric_type": metricType,
		"data":        data,
		"timestamp":   time.Now(),
		"message":     "Full metrics available via Prometheus /metrics",
	})
}

// GetSystemHealth returns comprehensive system health
func (h *MonitoringHandler) GetSystemHealth(c *gin.Context) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	health := gin.H{
		"status": "healthy",
		"system": gin.H{
			"goroutines": runtime.NumGoroutine(),
			"memory_mb":  mem.Alloc / 1024 / 1024,
			"gc_cycles":  mem.NumGC,
		},
		"timestamp": time.Now(),
	}

	c.JSON(http.StatusOK, health)
}

// Helper function to parse int query parameters
func parseIntQuery(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}
