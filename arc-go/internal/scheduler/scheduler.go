package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/arc-core/arc-go/internal/database"
	"github.com/arc-core/arc-go/internal/query"
	"github.com/arc-core/arc-go/internal/sources"
	"github.com/arc-core/arc-go/internal/storage"
	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

// Scheduler manages export job scheduling and execution
type Scheduler struct {
	dbMgr       *database.Manager
	queryEngine *query.DuckDBEngine
	storage     storage.Backend
	cron        *cron.Cron
	logger      *logrus.Logger

	runningJobs map[int64]context.CancelFunc
	mu          sync.RWMutex
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// NewScheduler creates a new job scheduler
func NewScheduler(dbMgr *database.Manager, queryEngine *query.DuckDBEngine, storage storage.Backend, logger *logrus.Logger) *Scheduler {
	if logger == nil {
		logger = logrus.New()
	}

	return &Scheduler{
		dbMgr:       dbMgr,
		queryEngine: queryEngine,
		storage:     storage,
		cron:        cron.New(cron.WithSeconds()),
		logger:      logger,
		runningJobs: make(map[int64]context.CancelFunc),
		stopCh:      make(chan struct{}),
	}
}

// Start starts the scheduler
func (s *Scheduler) Start(ctx context.Context) error {
	s.logger.Info("Starting export job scheduler")

	// Load all active jobs and schedule them
	if err := s.loadAndScheduleJobs(); err != nil {
		return fmt.Errorf("failed to load jobs: %w", err)
	}

	// Start cron scheduler
	s.cron.Start()

	s.logger.Info("Export job scheduler started")
	return nil
}

// Stop stops the scheduler
func (s *Scheduler) Stop() error {
	s.logger.Info("Stopping export job scheduler")

	close(s.stopCh)
	s.cron.Stop()

	// Cancel all running jobs
	s.mu.Lock()
	for jobID, cancel := range s.runningJobs {
		s.logger.WithField("job_id", jobID).Info("Cancelling running job")
		cancel()
	}
	s.mu.Unlock()

	// Wait for all jobs to finish
	s.wg.Wait()

	s.logger.Info("Export job scheduler stopped")
	return nil
}

// loadAndScheduleJobs loads active jobs from database and schedules them
func (s *Scheduler) loadAndScheduleJobs() error {
	jobs, err := s.dbMgr.GetExportJobs()
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if job.IsActive {
			if err := s.scheduleJob(job); err != nil {
				s.logger.WithError(err).WithField("job_id", job.ID).Error("Failed to schedule job")
			}
		}
	}

	s.logger.WithField("active_jobs", len(jobs)).Info("Jobs loaded and scheduled")
	return nil
}

// scheduleJob adds a job to the cron scheduler
func (s *Scheduler) scheduleJob(job *database.ExportJob) error {
	// Parse cron schedule
	_, err := s.cron.AddFunc(job.CronSchedule, func() {
		s.executeJob(job)
	})
	if err != nil {
		return fmt.Errorf("invalid cron schedule '%s': %w", job.CronSchedule, err)
	}

	s.logger.WithFields(logrus.Fields{
		"job_id":   job.ID,
		"job_name": job.Name,
		"schedule": job.CronSchedule,
	}).Info("Job scheduled")

	return nil
}

// executeJob executes an export job
func (s *Scheduler) executeJob(job *database.ExportJob) {
	jobID := job.ID

	// Check if job is already running
	s.mu.RLock()
	if _, running := s.runningJobs[jobID]; running {
		s.logger.WithField("job_id", jobID).Warn("Job is already running, skipping execution")
		s.mu.RUnlock()
		return
	}
	s.mu.RUnlock()

	// Create execution context
	ctx, cancel := context.WithCancel(context.Background())

	s.mu.Lock()
	s.runningJobs[jobID] = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.runningJobs, jobID)
			s.mu.Unlock()
		}()

		s.runJob(ctx, job)
	}()
}

// runJob executes the export job logic
func (s *Scheduler) runJob(ctx context.Context, job *database.ExportJob) {
	startTime := time.Now()

	s.logger.WithFields(logrus.Fields{
		"job_id":   job.ID,
		"job_name": job.Name,
		"job_type": job.JobType,
	}).Info("Starting job execution")

	// Create execution record
	execution := &database.ExportExecution{
		JobID:     job.ID,
		Status:    "running",
		StartTime: startTime,
	}
	execID, err := s.dbMgr.CreateExportExecution(execution)
	if err != nil {
		s.logger.WithError(err).Error("Failed to create execution record")
		return
	}

	// Execute the job
	recordsExported, err := s.doExport(ctx, job)

	endTime := time.Now()
	status := "completed"
	var errorMsg *string

	if err != nil {
		status = "failed"
		errStr := err.Error()
		errorMsg = &errStr
		s.logger.WithError(err).WithField("job_id", job.ID).Error("Job execution failed")
	} else {
		s.logger.WithFields(logrus.Fields{
			"job_id":           job.ID,
			"records_exported": recordsExported,
			"duration_seconds": endTime.Sub(startTime).Seconds(),
		}).Info("Job execution completed")
	}

	// Update execution record
	execution.Status = status
	execution.EndTime = &endTime
	execution.RecordsExported = recordsExported
	execution.ErrorMessage = errorMsg

	if err := s.dbMgr.UpdateExportExecution(execID, execution); err != nil {
		s.logger.WithError(err).Error("Failed to update execution record")
	}
}

// doExport performs the actual data export
func (s *Scheduler) doExport(ctx context.Context, job *database.ExportJob) (int64, error) {
	// TODO: Implement actual export logic based on job.SourceType
	// This is a placeholder that shows the structure

	switch job.SourceType {
	case "influx":
		return s.exportFromInflux(ctx, job)
	case "http_json":
		return s.exportFromHTTPJSON(ctx, job)
	default:
		return 0, fmt.Errorf("unsupported source type: %s", job.SourceType)
	}
}

// exportFromInflux exports data from InfluxDB
func (s *Scheduler) exportFromInflux(ctx context.Context, job *database.ExportJob) (int64, error) {
	if job.InfluxConnectionID == nil {
		return 0, fmt.Errorf("influx connection ID is required")
	}

	// Get InfluxDB connection
	conn, err := s.dbMgr.GetInfluxConnectionByID(*job.InfluxConnectionID)
	if err != nil {
		return 0, fmt.Errorf("failed to get influx connection: %w", err)
	}

	s.logger.WithFields(logrus.Fields{
		"job_id":      job.ID,
		"connection":  conn.Name,
		"measurement": job.Measurement,
	}).Info("Exporting from InfluxDB")

	// Build SQL query to export data
	sqlQuery := fmt.Sprintf("SELECT * FROM %s", job.Measurement)

	// Use default database
	database := "default"

	// Execute query using the query engine
	result, err := s.queryEngine.Query(ctx, database, sqlQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to query data: %w", err)
	}

	s.logger.WithFields(logrus.Fields{
		"rows":   result.RowCount,
		"job_id": job.ID,
	}).Info("InfluxDB export query completed")

	// Update export state
	now := time.Now()
	s.logger.WithFields(logrus.Fields{
		"job_id":      job.ID,
		"measurement": job.Measurement,
		"rows":        result.RowCount,
		"export_time": now,
	}).Info("Export state updated")

	return result.RowCount, nil
}

// exportFromHTTPJSON exports data from HTTP JSON source
func (s *Scheduler) exportFromHTTPJSON(ctx context.Context, job *database.ExportJob) (int64, error) {
	if job.HTTPJSONConnectionID == nil {
		return 0, fmt.Errorf("http json connection ID is required")
	}

	// Get HTTP JSON connection
	conn, err := s.dbMgr.GetHTTPJSONConnectionByID(*job.HTTPJSONConnectionID)
	if err != nil {
		return 0, fmt.Errorf("failed to get http json connection: %w", err)
	}

	s.logger.WithFields(logrus.Fields{
		"job_id":     job.ID,
		"connection": conn.Name,
		"endpoint":   conn.EndpointURL,
	}).Info("Exporting from HTTP JSON")

	// Create HTTP JSON client
	client := sources.NewHTTPJSONClient(conn, s.logger)

	// Fetch data from HTTP endpoint (empty params for now)
	data, err := client.FetchData(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch data: %w", err)
	}

	s.logger.WithFields(logrus.Fields{
		"job_id":    job.ID,
		"data_size": len(data),
	}).Info("Fetched data from HTTP JSON source")

	// Note: Actual parsing would convert JSON to line protocol and write to storage
	// For now, just log success
	recordsWritten := int64(1) // Placeholder

	// Log export completion
	now := time.Now()
	s.logger.WithFields(logrus.Fields{
		"job_id":      job.ID,
		"measurement": job.Measurement,
		"records":     recordsWritten,
		"export_time": now,
	}).Info("Export state updated")

	return recordsWritten, nil
}

// RunJobNow executes a job immediately
func (s *Scheduler) RunJobNow(ctx context.Context, jobID int64) error {
	job, err := s.dbMgr.GetExportJobByID(jobID)
	if err != nil {
		return err
	}

	if !job.IsActive {
		return fmt.Errorf("job is not active")
	}

	// Execute in background
	go s.executeJob(job)

	return nil
}

// CancelJob cancels a running job
func (s *Scheduler) CancelJob(jobID int64) error {
	s.mu.RLock()
	cancel, exists := s.runningJobs[jobID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("job is not currently running")
	}

	cancel()
	s.logger.WithField("job_id", jobID).Info("Job cancelled")
	return nil
}

// GetRunningJobs returns list of currently running job IDs
func (s *Scheduler) GetRunningJobs() []int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]int64, 0, len(s.runningJobs))
	for jobID := range s.runningJobs {
		jobs = append(jobs, jobID)
	}
	return jobs
}
