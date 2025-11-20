package v1

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/arc-core/arc-go/internal/config"
	"github.com/arc-core/arc-go/internal/database"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// AuthAdvancedHandler handles advanced authentication endpoints
type AuthAdvancedHandler struct {
	config    *config.Config
	dbMgr     *database.Manager
	logger    *logrus.Logger
	authCache map[string]*cacheEntry
}

type cacheEntry struct {
	token     string
	expiresAt time.Time
}

// NewAuthAdvancedHandler creates a new advanced auth handler
func NewAuthAdvancedHandler(cfg *config.Config, dbMgr *database.Manager, logger *logrus.Logger) *AuthAdvancedHandler {
	if logger == nil {
		logger = logrus.New()
	}

	return &AuthAdvancedHandler{
		config:    cfg,
		dbMgr:     dbMgr,
		logger:    logger,
		authCache: make(map[string]*cacheEntry),
	}
}

// CreateAPIToken creates a new API token with permissions
func (h *AuthAdvancedHandler) CreateAPIToken(c *gin.Context) {
	var req struct {
		Name        string   `json:"name" binding:"required"`
		Description string   `json:"description"`
		Permissions []string `json:"permissions"`
		ExpiresIn   int      `json:"expires_in"` // Days until expiration (0 = never)
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Generate token
	token := uuid.New().String()
	tokenHash := hashToken(token)

	// Calculate expiration
	var expiresAt *time.Time
	if req.ExpiresIn > 0 {
		exp := time.Now().Add(time.Duration(req.ExpiresIn) * 24 * time.Hour)
		expiresAt = &exp
	}

	// Convert permissions to JSON
	var permissionsJSON *string
	if len(req.Permissions) > 0 {
		permsStr := jsonArrayToString(req.Permissions)
		permissionsJSON = &permsStr
	}

	// Store in database
	apiToken := &database.APIToken{
		TokenHash:   tokenHash,
		Name:        req.Name,
		Description: stringPtr(req.Description),
		Permissions: permissionsJSON,
		ExpiresAt:   expiresAt,
	}

	id, err := h.dbMgr.CreateAPIToken(apiToken)
	if err != nil {
		h.logger.WithError(err).Error("Failed to create API token")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"token_id": id,
		"name":     req.Name,
	}).Info("API token created")

	c.JSON(http.StatusCreated, gin.H{
		"token_id":    id,
		"token":       token, // Only shown once!
		"name":        req.Name,
		"permissions": req.Permissions,
		"expires_at":  expiresAt,
		"message":     "WARNING: Save this token securely. It will not be shown again!",
	})
}

// ListAPITokens returns all API tokens (without actual token values)
func (h *AuthAdvancedHandler) ListAPITokens(c *gin.Context) {
	tokens, err := h.dbMgr.GetAPITokens()
	if err != nil {
		h.logger.WithError(err).Error("Failed to list API tokens")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Remove sensitive data
	sanitized := make([]map[string]interface{}, len(tokens))
	for i, token := range tokens {
		sanitized[i] = map[string]interface{}{
			"id":          token.ID,
			"name":        token.Name,
			"description": token.Description,
			"permissions": token.Permissions,
			"expires_at":  token.ExpiresAt,
			"last_used":   token.LastUsedAt,
			"created_at":  token.CreatedAt,
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"tokens": sanitized,
		"count":  len(sanitized),
	})
}

// GetAPIToken returns a specific API token details
func (h *AuthAdvancedHandler) GetAPIToken(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid token ID"})
		return
	}

	token, err := h.dbMgr.GetAPITokenByID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Token not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":          token.ID,
		"name":        token.Name,
		"description": token.Description,
		"permissions": token.Permissions,
		"expires_at":  token.ExpiresAt,
		"last_used":   token.LastUsedAt,
		"created_at":  token.CreatedAt,
	})
}

// UpdateAPIToken updates an API token's metadata
func (h *AuthAdvancedHandler) UpdateAPIToken(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid token ID"})
		return
	}

	var req struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Permissions []string `json:"permissions"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Get existing token
	token, err := h.dbMgr.GetAPITokenByID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Token not found"})
		return
	}

	// Update fields
	if req.Name != "" {
		token.Name = req.Name
	}
	if req.Description != "" {
		desc := req.Description
		token.Description = &desc
	}
	if len(req.Permissions) > 0 {
		perms := jsonArrayToString(req.Permissions)
		token.Permissions = &perms
	}

	if err := h.dbMgr.UpdateAPIToken(id, token); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message":  "Token updated successfully",
		"token_id": id,
	})
}

// DeleteAPIToken deletes an API token
func (h *AuthAdvancedHandler) DeleteAPIToken(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid token ID"})
		return
	}

	if err := h.dbMgr.DeleteAPIToken(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.WithField("token_id", id).Info("API token deleted")

	c.JSON(http.StatusOK, gin.H{
		"message":  "Token deleted successfully",
		"token_id": id,
	})
}

// RotateAPIToken rotates an API token (generates new token value)
func (h *AuthAdvancedHandler) RotateAPIToken(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid token ID"})
		return
	}

	// Get existing token
	token, err := h.dbMgr.GetAPITokenByID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Token not found"})
		return
	}

	// Generate new token
	newToken := uuid.New().String()
	newTokenHash := hashToken(newToken)

	// Update token hash
	token.TokenHash = newTokenHash

	if err := h.dbMgr.UpdateAPIToken(id, token); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Clear auth cache
	h.clearAuthCache()

	h.logger.WithField("token_id", id).Info("API token rotated")

	c.JSON(http.StatusOK, gin.H{
		"message":  "Token rotated successfully",
		"token_id": id,
		"token":    newToken, // Only shown once!
		"warning":  "WARNING: Update your applications with the new token immediately!",
	})
}

// GetAuthCacheStats returns auth cache statistics
func (h *AuthAdvancedHandler) GetAuthCacheStats(c *gin.Context) {
	now := time.Now()
	active := 0
	expired := 0

	for _, entry := range h.authCache {
		if entry.expiresAt.After(now) {
			active++
		} else {
			expired++
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"total_entries":   len(h.authCache),
		"active_entries":  active,
		"expired_entries": expired,
	})
}

// InvalidateAuthCache clears the auth cache
func (h *AuthAdvancedHandler) InvalidateAuthCache(c *gin.Context) {
	count := len(h.authCache)
	h.clearAuthCache()

	h.logger.WithField("entries_cleared", count).Info("Auth cache invalidated")

	c.JSON(http.StatusOK, gin.H{
		"message":         "Auth cache invalidated",
		"entries_cleared": count,
	})
}

// Helper functions

func (h *AuthAdvancedHandler) clearAuthCache() {
	h.authCache = make(map[string]*cacheEntry)
}

func hashToken(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func jsonArrayToString(arr []string) string {
	result := "["
	for i, s := range arr {
		if i > 0 {
			result += ","
		}
		result += `"` + s + `"`
	}
	result += "]"
	return result
}
