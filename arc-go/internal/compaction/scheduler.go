package compaction

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// Scheduler manages automatic compaction scheduling
type Scheduler struct {
	config    *config.CompactionConfig
	storage   storage.Backend
	compactor *Compactor
	cron      *cron.Cron
	logger    *logrus.Logger
	mu        sync.RWMutex
	isRunning bool
	jobID     cron.EntryID
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	// Metrics
	metrics SchedulerMetrics
}

// SchedulerMetrics holds scheduler performance metrics
type SchedulerMetrics struct {
	JobsScheduled    int64         `json:"jobs_scheduled"`
	JobsCompleted    int64         `json:"jobs_completed"`
	JobsFailed       int64         `json:"jobs_failed"`
	LastRun          time.Time     `json:"last_run"`
	NextRun          time.Time     `json:"next_run"`
	TotalRunTime     time.Duration `json:"total_run_time"`
	AverageRunTime   time.Duration `json:"average_run_time"`
	ActiveJobs       int           `json:"active_jobs"`
	FilesCompacted   int64         `json:"files_compacted"`
	BytesProcessed   int64         `json:"bytes_processed"`
	CompressionRatio float64       `json:"compression_ratio"`
}

// NewScheduler creates a new compaction scheduler
func NewScheduler(
	cfg *config.CompactionConfig,
	storage storage.Backend,
	compactor *Compactor,
	logger *logrus.Logger,
) *Scheduler {
	if logger == nil {
		logger = logrus.New()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create cron scheduler
	c := cron.New(cron.WithSeconds())

	scheduler := &Scheduler{
		config:    cfg,
		storage:   storage,
		compactor: compactor,
		cron:      c,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
		metrics:   SchedulerMetrics{},
	}

	return scheduler
}

// Start starts the compaction scheduler
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		return fmt.Errorf("scheduler is already running")
	}

	if !s.config.Enabled {
		s.logger.Info("Compaction scheduler is disabled")
		return nil
	}

	s.logger.WithFields(logrus.Fields{
		"schedule":            s.config.Schedule,
		"max_concurrent_jobs": s.config.MaxConcurrentJobs,
		"min_age_hours":       s.config.MinAgeHours,
		"min_files":           s.config.MinFiles,
		"target_file_size_mb": s.config.TargetFileSizeMB,
	}).Info("Starting compaction scheduler")

	// Schedule the job
	jobID, err := s.cron.AddFunc(s.config.Schedule, s.runCompaction)
	if err != nil {
		return fmt.Errorf("failed to schedule compaction job: %w", err)
	}

	s.jobID = jobID
	s.isRunning = true
	s.cron.Start()

	// Update next run time
	if entry := s.cron.Entry(jobID); entry.ID != 0 {
		s.metrics.NextRun = entry.Next
	}

	s.logger.Info("Compaction scheduler started successfully")
	return nil
}

// Stop stops the compaction scheduler
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return nil
	}

	s.logger.Info("Stopping compaction scheduler")

	// Cancel context
	s.cancel()

	// Stop cron scheduler
	ctx := s.cron.Stop()
	<-ctx.Done()

	// Wait for background jobs to finish
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("Compaction scheduler stopped successfully")
	case <-time.After(30 * time.Second):
		s.logger.Warn("Compaction scheduler stop timeout")
	}

	s.isRunning = false
	return nil
}

// Trigger triggers an immediate compaction run
func (s *Scheduler) Trigger() error {
	if !s.config.Enabled {
		return fmt.Errorf("compaction is disabled")
	}

	s.logger.Info("Triggering manual compaction")
	go s.runCompaction()
	return nil
}

// runCompaction executes the compaction process
func (s *Scheduler) runCompaction() {
	if !s.shouldRun() {
		s.logger.Debug("Skipping compaction run")
		return
	}

	s.wg.Add(1)
	defer s.wg.Done()

	start := time.Now()
	s.mu.Lock()
	s.metrics.JobsScheduled++
	s.metrics.ActiveJobs++
	s.metrics.LastRun = start
	s.mu.Unlock()

	s.logger.Info("Starting compaction run")

	// Execute compaction
	result, err := s.compactor.Compact(s.ctx)
	if err != nil {
		s.mu.Lock()
		s.metrics.JobsFailed++
		s.metrics.ActiveJobs--
		s.mu.Unlock()

		s.logger.WithError(err).Error("Compaction run failed")
		return
	}

	// Update metrics
	runTime := time.Since(start)
	s.mu.Lock()
	s.metrics.JobsCompleted++
	s.metrics.ActiveJobs--
	s.metrics.FilesCompacted += result.FilesCompacted
	s.metrics.BytesProcessed += result.BytesProcessed
	s.metrics.TotalRunTime += runTime

	// Update average run time
	if s.metrics.JobsCompleted > 0 {
		s.metrics.AverageRunTime = s.metrics.TotalRunTime / time.Duration(s.metrics.JobsCompleted)
	}

	// Update compression ratio
	if result.BytesProcessed > 0 {
		s.metrics.CompressionRatio = float64(result.BytesWritten) / float64(result.BytesProcessed)
	}
	s.mu.Unlock()

	// Update next run time
	if entry := s.cron.Entry(s.jobID); entry.ID != 0 {
		s.metrics.NextRun = entry.Next
	}

	s.logger.WithFields(logrus.Fields{
		"duration":          runTime,
		"files_compacted":   result.FilesCompacted,
		"bytes_processed":   result.BytesProcessed,
		"bytes_written":     result.BytesWritten,
		"compression_ratio": s.metrics.CompressionRatio,
		"partitions":        len(result.PartitionsCompacted),
	}).Info("Compaction run completed successfully")
}

// shouldRun checks if compaction should run based on current conditions
func (s *Scheduler) shouldRun() bool {
	// Check if concurrent job limit is reached
	s.mu.RLock()
	activeJobs := s.metrics.ActiveJobs
	s.mu.RUnlock()

	if activeJobs >= s.config.MaxConcurrentJobs {
		s.logger.WithFields(logrus.Fields{
			"active_jobs":         activeJobs,
			"max_concurrent_jobs": s.config.MaxConcurrentJobs,
		}).Debug("Skipping compaction due to concurrent job limit")
		return false
	}

	return true
}

// GetStatus returns the current scheduler status
func (s *Scheduler) GetStatus() SchedulerStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status := SchedulerStatus{
		IsRunning:  s.isRunning,
		Schedule:   s.config.Schedule,
		NextRun:    s.metrics.NextRun,
		LastRun:    s.metrics.LastRun,
		ActiveJobs: s.metrics.ActiveJobs,
		Enabled:    s.config.Enabled,
		Metrics:    s.metrics,
		Config:     *s.config,
	}

	return status
}

// GetMetrics returns scheduler metrics
func (s *Scheduler) GetMetrics() SchedulerMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metrics
}

// ResetMetrics resets scheduler metrics
func (s *Scheduler) ResetMetrics() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = SchedulerMetrics{}
}

// UpdateConfig updates the scheduler configuration
func (s *Scheduler) UpdateConfig(cfg *config.CompactionConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	wasRunning := s.isRunning

	// Stop if running
	if wasRunning {
		if err := s.Stop(); err != nil {
			return fmt.Errorf("failed to stop scheduler: %w", err)
		}
	}

	// Update configuration
	s.config = cfg

	// Restart if it was running and is still enabled
	if wasRunning && cfg.Enabled {
		return s.Start()
	}

	return nil
}

// GetJobHistory returns recent job history (simplified)
func (s *Scheduler) GetJobHistory(limit int) []JobInfo {
	// This would typically query a database or log files
	// For now, return empty slice
	return []JobInfo{}
}

// SchedulerStatus represents the current scheduler status
type SchedulerStatus struct {
	IsRunning  bool                    `json:"is_running"`
	Schedule   string                  `json:"schedule"`
	NextRun    time.Time               `json:"next_run"`
	LastRun    time.Time               `json:"last_run"`
	ActiveJobs int                     `json:"active_jobs"`
	Enabled    bool                    `json:"enabled"`
	Metrics    SchedulerMetrics        `json:"metrics"`
	Config     config.CompactionConfig `json:"config"`
}

// JobInfo represents information about a compaction job
type JobInfo struct {
	JobID      string    `json:"job_id"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time"`
	Status     string    `json:"status"`
	Partitions int       `json:"partitions"`
	Files      int       `json:"files"`
	Bytes      int64     `json:"bytes"`
	Error      string    `json:"error,omitempty"`
}

// Close closes the scheduler
func (s *Scheduler) Close() error {
	return s.Stop()
}

// Health checks the health of the scheduler
func (s *Scheduler) Health() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.config.Enabled && !s.isRunning {
		return fmt.Errorf("compaction is enabled but scheduler is not running")
	}

	// Check if cron scheduler is healthy
	if s.isRunning && s.cron == nil {
		return fmt.Errorf("scheduler is running but cron scheduler is nil")
	}

	return nil
}
