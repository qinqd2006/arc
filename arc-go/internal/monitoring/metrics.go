package monitoring

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

// Monitor handles metrics collection and exposition
type Monitor struct {
	config   *MonitoringConfig
	logger   *logrus.Logger
	registry *prometheus.Registry
	server   *http.Server
	mu       sync.RWMutex
	started  bool

	// Custom metrics
	requestDuration   *prometheus.HistogramVec
	requestTotal      *prometheus.CounterVec
	requestErrors     *prometheus.CounterVec
	ingestionRate     *prometheus.GaugeVec
	ingestionTotal    *prometheus.CounterVec
	storageOperations *prometheus.CounterVec
	queryDuration     *prometheus.HistogramVec
	queryTotal        *prometheus.CounterVec
	queryErrors       *prometheus.CounterVec
	bufferSize        *prometheus.GaugeVec
	bufferTotal       *prometheus.CounterVec
	compactionJobs    *prometheus.CounterVec
	walOperations     *prometheus.CounterVec
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	Enabled            bool          `toml:"enabled"`
	CollectionInterval time.Duration `toml:"collection_interval"`
	MetricsPath        string        `toml:"metrics_path"`
	Port               int           `toml:"port"`
}

// NewMonitor creates a new monitoring instance
func NewMonitor(cfg *MonitoringConfig, logger *logrus.Logger) *Monitor {
	if logger == nil {
		logger = logrus.New()
	}

	// Set defaults
	if cfg.MetricsPath == "" {
		cfg.MetricsPath = "/metrics"
	}
	if cfg.Port <= 0 {
		cfg.Port = 9090
	}

	// Create Prometheus registry
	registry := prometheus.NewRegistry()

	// Add default Go metrics
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	monitor := &Monitor{
		config:   cfg,
		logger:   logger,
		registry: registry,
	}

	// Initialize custom metrics
	monitor.initMetrics()

	return monitor
}

// initMetrics initializes all custom Prometheus metrics
func (m *Monitor) initMetrics() {
	// HTTP request metrics
	m.requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "arc_http_request_duration_seconds",
			Help:    "Duration of HTTP requests in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint", "status"},
	)

	m.requestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	m.requestErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_http_request_errors_total",
			Help: "Total number of HTTP request errors",
		},
		[]string{"method", "endpoint", "error_type"},
	)

	// Ingestion metrics
	m.ingestionRate = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arc_ingestion_rate_records_per_second",
			Help: "Current ingestion rate in records per second",
		},
		[]string{"database", "measurement", "format"},
	)

	m.ingestionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_ingestion_records_total",
			Help: "Total number of records ingested",
		},
		[]string{"database", "measurement", "format"},
	)

	// Storage metrics
	m.storageOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_storage_operations_total",
			Help: "Total number of storage operations",
		},
		[]string{"backend", "operation", "status"},
	)

	// Query metrics
	m.queryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "arc_query_duration_seconds",
			Help:    "Duration of queries in seconds",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300},
		},
		[]string{"database", "query_type"},
	)

	m.queryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_queries_total",
			Help: "Total number of queries",
		},
		[]string{"database", "query_type", "status"},
	)

	m.queryErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_query_errors_total",
			Help: "Total number of query errors",
		},
		[]string{"database", "error_type"},
	)

	// Buffer metrics
	m.bufferSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "arc_buffer_size_records",
			Help: "Current buffer size in records",
		},
		[]string{"database", "measurement"},
	)

	m.bufferTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_buffer_records_total",
			Help: "Total number of records processed by buffer",
		},
		[]string{"database", "measurement", "operation"},
	)

	// Compaction metrics
	m.compactionJobs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_compaction_jobs_total",
			Help: "Total number of compaction jobs",
		},
		[]string{"database", "measurement", "status"},
	)

	// WAL metrics
	m.walOperations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "arc_wal_operations_total",
			Help: "Total number of WAL operations",
		},
		[]string{"operation", "status"},
	)

	// Register metrics
	m.registry.MustRegister(
		m.requestDuration,
		m.requestTotal,
		m.requestErrors,
		m.ingestionRate,
		m.ingestionTotal,
		m.storageOperations,
		m.queryDuration,
		m.queryTotal,
		m.queryErrors,
		m.bufferSize,
		m.bufferTotal,
		m.compactionJobs,
		m.walOperations,
	)
}

// Start starts the monitoring server
func (m *Monitor) Start(ctx context.Context) error {
	if !m.config.Enabled {
		m.logger.Info("Monitoring is disabled")
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return fmt.Errorf("monitoring is already started")
	}

	// Create HTTP server for metrics
	mux := http.NewServeMux()
	mux.Handle(m.config.MetricsPath, promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))

	// Add health endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	m.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", m.config.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		m.logger.WithField("port", m.config.Port).Info("Starting metrics server")
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.WithError(err).Error("Metrics server failed")
		}
	}()

	m.started = true

	// Start metrics collection goroutine
	go m.collectMetrics(ctx)

	m.logger.WithFields(logrus.Fields{
		"port":         m.config.Port,
		"metrics_path": m.config.MetricsPath,
	}).Info("Monitoring started")

	return nil
}

// Stop stops the monitoring server
func (m *Monitor) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil
	}

	m.logger.Info("Stopping monitoring")

	// Shutdown server
	if m.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := m.server.Shutdown(ctx); err != nil {
			m.logger.WithError(err).Error("Failed to shutdown metrics server gracefully")
			return err
		}
	}

	m.started = false
	m.logger.Info("Monitoring stopped")
	return nil
}

// PrometheusHandler returns the Prometheus HTTP handler
func (m *Monitor) PrometheusHandler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RecordHTTPRequest records HTTP request metrics
func (m *Monitor) RecordHTTPRequest(method, endpoint, status string, duration time.Duration) {
	if !m.config.Enabled {
		return
	}

	m.requestTotal.WithLabelValues(method, endpoint, status).Inc()
	m.requestDuration.WithLabelValues(method, endpoint, status).Observe(duration.Seconds())

	if duration > 5*time.Second {
		m.logger.WithFields(logrus.Fields{
			"method":   method,
			"endpoint": endpoint,
			"duration": duration,
		}).Warn("Slow HTTP request detected")
	}
}

// RecordHTTPRequestError records HTTP request error
func (m *Monitor) RecordHTTPRequestError(method, endpoint, errorType string) {
	if !m.config.Enabled {
		return
	}

	m.requestErrors.WithLabelValues(method, endpoint, errorType).Inc()
}

// RecordIngestion records ingestion metrics
func (m *Monitor) RecordIngestion(database, measurement, format string, records int64, rate float64) {
	if !m.config.Enabled {
		return
	}

	m.ingestionTotal.WithLabelValues(database, measurement, format).Add(float64(records))
	m.ingestionRate.WithLabelValues(database, measurement, format).Set(rate)
}

// RecordStorageOperation records storage operation metrics
func (m *Monitor) RecordStorageOperation(backend, operation, status string) {
	if !m.config.Enabled {
		return
	}

	m.storageOperations.WithLabelValues(backend, operation, status).Inc()
}

// RecordQuery records query metrics
func (m *Monitor) RecordQuery(database, queryType, status string, duration time.Duration) {
	if !m.config.Enabled {
		return
	}

	m.queryTotal.WithLabelValues(database, queryType, status).Inc()
	m.queryDuration.WithLabelValues(database, queryType).Observe(duration.Seconds())

	if duration > 30*time.Second {
		m.logger.WithFields(logrus.Fields{
			"database":   database,
			"query_type": queryType,
			"duration":   duration,
		}).Warn("Slow query detected")
	}
}

// RecordQueryError records query error
func (m *Monitor) RecordQueryError(database, errorType string) {
	if !m.config.Enabled {
		return
	}

	m.queryErrors.WithLabelValues(database, errorType).Inc()
}

// UpdateBufferSize updates buffer size gauge
func (m *Monitor) UpdateBufferSize(database, measurement string, size int64) {
	if !m.config.Enabled {
		return
	}

	m.bufferSize.WithLabelValues(database, measurement).Set(float64(size))
}

// RecordBufferOperation records buffer operation metrics
func (m *Monitor) RecordBufferOperation(database, measurement, operation string, records int64) {
	if !m.config.Enabled {
		return
	}

	m.bufferTotal.WithLabelValues(database, measurement, operation).Add(float64(records))
}

// RecordCompactionJob records compaction job metrics
func (m *Monitor) RecordCompactionJob(database, measurement, status string) {
	if !m.config.Enabled {
		return
	}

	m.compactionJobs.WithLabelValues(database, measurement, status).Inc()
}

// RecordWALOperation records WAL operation metrics
func (m *Monitor) RecordWALOperation(operation, status string) {
	if !m.config.Enabled {
		return
	}

	m.walOperations.WithLabelValues(operation, status).Inc()
}

// collectMetrics periodically collects system metrics
func (m *Monitor) collectMetrics(ctx context.Context) {
	if !m.config.Enabled {
		return
	}

	ticker := time.NewTicker(m.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.collectSystemMetrics()
		}
	}
}

// collectSystemMetrics collects system-level metrics
func (m *Monitor) collectSystemMetrics() {
	// This would collect system metrics like CPU, memory, disk usage
	// For now, it's a placeholder that could be extended

	// Example: Log current memory usage (could be sent to Prometheus)
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	m.logger.WithFields(logrus.Fields{
		"alloc_mb":   memStats.Alloc / 1024 / 1024,
		"sys_mb":     memStats.Sys / 1024 / 1024,
		"num_gc":     memStats.NumGC,
		"goroutines": runtime.NumGoroutine(),
	}).Debug("System metrics collected")
}

// GetMetrics returns current metrics snapshot
func (m *Monitor) GetMetrics() (map[string]interface{}, error) {
	if !m.config.Enabled {
		return nil, fmt.Errorf("monitoring is disabled")
	}

	// This would gather current metric values from Prometheus registry
	// For now, return a placeholder
	return map[string]interface{}{
		"monitoring_enabled":  true,
		"collection_interval": m.config.CollectionInterval.String(),
		"metrics_path":        m.config.MetricsPath,
		"port":                m.config.Port,
	}, nil
}

// Health returns the health status of the monitoring system
func (m *Monitor) Health() error {
	if !m.config.Enabled {
		return nil // Monitoring is disabled, not unhealthy
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.started {
		return fmt.Errorf("monitoring is enabled but not started")
	}

	if m.server == nil {
		return fmt.Errorf("monitoring server is nil")
	}

	return nil
}
