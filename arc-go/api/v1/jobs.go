package v1

import (
	"context"
	"net/http"
	"strconv"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/arc-core/arc-go/internal/scheduler"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// JobsHandler handles export job management endpoints
type JobsHandler struct {
	config    *config.Config
	dbMgr     *database.Manager
	scheduler *scheduler.Scheduler
	logger    *logrus.Logger
}

// NewJobsHandler creates a new jobs handler
func NewJobsHandler(cfg *config.Config, dbMgr *database.Manager, sched *scheduler.Scheduler, logger *logrus.Logger) *JobsHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &JobsHandler{
		config:    cfg,
		dbMgr:     dbMgr,
		scheduler: sched,
		logger:    logger,
	}
}

// GetJobs returns all export jobs
func (h *JobsHandler) GetJobs(c *gin.Context) {
	jobs, err := h.dbMgr.GetExportJobs()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get export jobs")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, jobs)
}

// CreateJob creates a new export job
func (h *JobsHandler) CreateJob(c *gin.Context) {
	var job database.ExportJob
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id, err := h.dbMgr.CreateExportJob(&job)
	if err != nil {
		h.logger.WithError(err).Error("Failed to create export job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"job_id":   id,
		"job_name": job.Name,
	}).Info("Export job created")

	c.JSON(http.StatusCreated, gin.H{
		"id":      id,
		"message": "Export job created successfully",
	})
}

// UpdateJob updates an existing export job
func (h *JobsHandler) UpdateJob(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	var job database.ExportJob
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.dbMgr.UpdateExportJob(id, &job); err != nil {
		h.logger.WithError(err).Error("Failed to update export job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("job_id", id).Info("Export job updated")
	c.JSON(http.StatusOK, gin.H{"message": "Export job updated successfully"})
}

// DeleteJob deletes an export job
func (h *JobsHandler) DeleteJob(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	if err := h.dbMgr.DeleteExportJob(id); err != nil {
		h.logger.WithError(err).Error("Failed to delete export job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("job_id", id).Info("Export job deleted")
	c.JSON(http.StatusOK, gin.H{"message": "Export job deleted successfully"})
}

// RunJobNow triggers immediate execution of a job
func (h *JobsHandler) RunJobNow(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	if err := h.scheduler.RunJobNow(context.Background(), id); err != nil {
		h.logger.WithError(err).WithField("job_id", id).Error("Failed to run job")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("job_id", id).Info("Job execution triggered")
	c.JSON(http.StatusOK, gin.H{
		"message": "Job execution started in background",
		"job_id":  id,
		"status":  "triggered",
	})
}

// CancelJob cancels a running job
func (h *JobsHandler) CancelJob(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	if err := h.scheduler.CancelJob(id); err != nil {
		h.logger.WithError(err).WithField("job_id", id).Warn("Failed to cancel job")
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("job_id", id).Info("Job cancelled")
	c.JSON(http.StatusOK, gin.H{
		"message": "Job cancelled successfully",
		"job_id":  id,
	})
}

// GetJobExecutions returns execution history for a job
func (h *JobsHandler) GetJobExecutions(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid job ID"})
		return
	}

	limit, err := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if err != nil || limit < 1 || limit > 1000 {
		limit = 50
	}

	executions, err := h.dbMgr.GetExportExecutions(id, limit)
	if err != nil {
		h.logger.WithError(err).Error("Failed to get job executions")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, executions)
}

// GetJobMonitoring returns monitoring data for all jobs
func (h *JobsHandler) GetJobMonitoring(c *gin.Context) {
	jobs, err := h.dbMgr.GetExportJobs()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get jobs for monitoring")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	runningJobs := h.scheduler.GetRunningJobs()

	// Get recent executions for each job
	var allExecutions []*database.ExportExecution
	for _, job := range jobs {
		executions, err := h.dbMgr.GetExportExecutions(job.ID, 20)
		if err == nil {
			allExecutions = append(allExecutions, executions...)
		}
	}

	activeJobs := 0
	for _, job := range jobs {
		if job.IsActive {
			activeJobs++
		}
	}

	failedExecutions := 0
	for _, exec := range allExecutions {
		if exec.Status == "failed" {
			failedExecutions++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"jobs":              jobs,
		"recent_executions": allExecutions,
		"stats": gin.H{
			"total_jobs":   len(jobs),
			"active_jobs":  activeJobs,
			"running_jobs": len(runningJobs),
			"failed_jobs":  failedExecutions,
		},
	})
}
