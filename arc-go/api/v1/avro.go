package v1

import (
	"net/http"
	"strconv"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// AvroHandler handles Avro schema management endpoints
type AvroHandler struct {
	config *config.Config
	dbMgr  *database.Manager
	logger *logrus.Logger
}

// NewAvroHandler creates a new Avro handler
func NewAvroHandler(cfg *config.Config, dbMgr *database.Manager, logger *logrus.Logger) *AvroHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &AvroHandler{
		config: cfg,
		dbMgr:  dbMgr,
		logger: logger,
	}
}

// GetSchemas returns all Avro schemas
func (h *AvroHandler) GetSchemas(c *gin.Context) {
	schemas, err := h.dbMgr.GetAvroSchemas()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get Avro schemas")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"schemas": schemas})
}

// CreateSchema creates a new Avro schema
func (h *AvroHandler) CreateSchema(c *gin.Context) {
	var schema database.AvroSchema
	if err := c.ShouldBindJSON(&schema); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id, err := h.dbMgr.AddAvroSchema(&schema)
	if err != nil {
		h.logger.WithError(err).Error("Failed to create Avro schema")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"schema_id":   id,
		"schema_name": schema.SchemaName,
		"topic":       schema.TopicPattern,
	}).Info("Avro schema created")

	c.JSON(http.StatusCreated, gin.H{
		"message":   "Avro schema created",
		"schema_id": id,
	})
}

// GetSchema returns a specific Avro schema by ID
func (h *AvroHandler) GetSchema(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schema ID"})
		return
	}

	schemas, err := h.dbMgr.GetAvroSchemas()
	if err != nil {
		h.logger.WithError(err).Error("Failed to get Avro schemas")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	for _, schema := range schemas {
		if schema.ID == id {
			c.JSON(http.StatusOK, schema)
			return
		}
	}

	c.JSON(http.StatusNotFound, gin.H{"error": "Avro schema not found"})
}

// UpdateSchema updates an existing Avro schema
func (h *AvroHandler) UpdateSchema(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schema ID"})
		return
	}

	var schema database.AvroSchema
	if err := c.ShouldBindJSON(&schema); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := h.dbMgr.UpdateAvroSchema(id, &schema); err != nil {
		h.logger.WithError(err).Error("Failed to update Avro schema")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("schema_id", id).Info("Avro schema updated")
	c.JSON(http.StatusOK, gin.H{
		"message":   "Avro schema updated",
		"schema_id": id,
	})
}

// DeleteSchema deletes an Avro schema
func (h *AvroHandler) DeleteSchema(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid schema ID"})
		return
	}

	if err := h.dbMgr.DeleteAvroSchema(id); err != nil {
		h.logger.WithError(err).Error("Failed to delete Avro schema")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("schema_id", id).Info("Avro schema deleted")
	c.JSON(http.StatusOK, gin.H{
		"message":   "Avro schema deleted",
		"schema_id": id,
	})
}

// GetSchemaForTopic finds the best matching Avro schema for a topic
func (h *AvroHandler) GetSchemaForTopic(c *gin.Context) {
	topic := c.Param("topic")
	if topic == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic name required"})
		return
	}

	schema, err := h.dbMgr.GetAvroSchemaForTopic(topic)
	if err != nil {
		h.logger.WithError(err).Error("Failed to get Avro schema for topic")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	if schema == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "No Avro schema found for topic",
			"topic": topic,
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"topic":       topic,
		"schema_name": schema.SchemaName,
		"pattern":     schema.TopicPattern,
	}).Debug("Avro schema matched for topic")

	c.JSON(http.StatusOK, schema)
}
