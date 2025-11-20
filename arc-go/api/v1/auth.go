package v1

import (
	"net/http"
	"time"

	"github.com/arc-core/arc-go/internal/auth"
	"github.com/arc-core/arc-go/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// AuthHandler handles authentication endpoints
type AuthHandler struct {
	config     *config.Config
	jwtManager *auth.JWTManager
	logger     *logrus.Logger
}

// NewAuthHandler creates a new authentication handler
func NewAuthHandler(cfg *config.Config, logger *logrus.Logger) *AuthHandler {
	if logger == nil {
		logger = logrus.New()
	}

	// Create JWT manager
	jwtManager := auth.NewJWTManager(cfg.Auth.DefaultToken, 24*time.Hour, logger)

	return &AuthHandler{
		config:     cfg,
		jwtManager: jwtManager,
		logger:     logger,
	}
}

// CreateTokenRequest represents a token creation request
type CreateTokenRequest struct {
	AppName     string `json:"app_name" binding:"required"`
	ExpiresIn   int    `json:"expires_in,omitempty"` // Duration in hours
	Description string `json:"description,omitempty"`
}

// CreateTokenResponse represents a token creation response
type CreateTokenResponse struct {
	TokenID   string    `json:"token_id"`
	AppName   string    `json:"app_name"`
	Token     string    `json:"token"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	ExpiresIn int       `json:"expires_in"` // Duration in seconds
}

// VerifyTokenResponse represents a token verification response
type VerifyTokenResponse struct {
	Valid     bool      `json:"valid"`
	TokenID   string    `json:"token_id,omitempty"`
	AppName   string    `json:"app_name,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	ExpiresIn int       `json:"expires_in,omitempty"` // Duration in seconds
	Error     string    `json:"error,omitempty"`
}

// RefreshTokenRequest represents a token refresh request
type RefreshTokenRequest struct {
	Token     string `json:"token" binding:"required"`
	ExpiresIn int    `json:"expires_in,omitempty"` // Duration in hours
}

// ExtendTokenRequest represents a token extension request
type ExtendTokenRequest struct {
	Token           string `json:"token" binding:"required"`
	AdditionalHours int    `json:"additional_hours" binding:"required"`
}

// CreateToken creates a new authentication token
func (h *AuthHandler) CreateToken(c *gin.Context) {
	var req CreateTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.WithError(err).Error("Invalid create token request")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"app_name":    req.AppName,
		"expires_in":  req.ExpiresIn,
		"description": req.Description,
		"remote_addr": c.ClientIP(),
	}).Debug("Auth: Creating token")

	// Set token duration
	tokenDuration := 24 * time.Hour // Default
	if req.ExpiresIn > 0 {
		tokenDuration = time.Duration(req.ExpiresIn) * time.Hour
	}

	// Create temporary JWT manager with custom duration
	tempJWTManager := auth.NewJWTManager(h.config.Auth.DefaultToken, tokenDuration, h.logger)

	// Generate token
	tokenInfo, err := tempJWTManager.GenerateToken(req.AppName, req.Description)
	if err != nil {
		h.logger.WithError(err).Error("Failed to generate token")
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":   "Failed to generate token",
			"details": err.Error(),
		})
		return
	}

	response := CreateTokenResponse{
		TokenID:   tokenInfo.TokenID,
		AppName:   tokenInfo.AppName,
		Token:     tokenInfo.Token,
		CreatedAt: tokenInfo.CreatedAt,
		ExpiresAt: tokenInfo.ExpiresAt,
		ExpiresIn: int(tokenDuration.Seconds()),
	}

	h.logger.WithFields(logrus.Fields{
		"token_id":   tokenInfo.TokenID,
		"app_name":   tokenInfo.AppName,
		"expires_at": tokenInfo.ExpiresAt,
	}).Info("Auth: Token created successfully")

	c.JSON(http.StatusCreated, response)
}

// VerifyToken verifies an authentication token
func (h *AuthHandler) VerifyToken(c *gin.Context) {
	token := c.Query("token")
	if token == "" {
		token = c.GetHeader("Authorization")
		if token != "" {
			// Remove "Bearer " prefix
			token = token[7:]
		}
	}

	if token == "" {
		c.JSON(http.StatusBadRequest, VerifyTokenResponse{
			Valid: false,
			Error: "Token is required",
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"token_length": len(token),
		"remote_addr":  c.ClientIP(),
	}).Debug("Auth: Verifying token")

	// Validate token
	claims, err := h.jwtManager.ValidateToken(token)
	if err != nil {
		c.JSON(http.StatusOK, VerifyTokenResponse{
			Valid: false,
			Error: err.Error(),
		})
		return
	}

	// Calculate remaining time
	remainingTime := time.Until(time.Unix(claims.ExpiresAt, 0))
	if remainingTime < 0 {
		remainingTime = 0
	}

	response := VerifyTokenResponse{
		Valid:     true,
		TokenID:   claims.TokenID,
		AppName:   claims.AppName,
		CreatedAt: time.Unix(claims.CreatedAt, 0),
		ExpiresAt: time.Unix(claims.ExpiresAt, 0),
		ExpiresIn: int(remainingTime.Seconds()),
	}

	h.logger.WithFields(logrus.Fields{
		"token_id": claims.TokenID,
		"app_name": claims.AppName,
		"valid":    true,
	}).Debug("Auth: Token verified successfully")

	c.JSON(http.StatusOK, response)
}

// RefreshToken refreshes an existing token
func (h *AuthHandler) RefreshToken(c *gin.Context) {
	var req RefreshTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"expires_in":  req.ExpiresIn,
		"remote_addr": c.ClientIP(),
	}).Debug("Auth: Refreshing token")

	// Set token duration
	tokenDuration := 24 * time.Hour // Default
	if req.ExpiresIn > 0 {
		tokenDuration = time.Duration(req.ExpiresIn) * time.Hour
	}

	// Create temporary JWT manager with custom duration
	tempJWTManager := auth.NewJWTManager(h.config.Auth.DefaultToken, tokenDuration, h.logger)

	// Refresh token
	tokenInfo, err := tempJWTManager.RefreshToken(req.Token)
	if err != nil {
		h.logger.WithError(err).Error("Failed to refresh token")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Failed to refresh token",
			"details": err.Error(),
		})
		return
	}

	response := CreateTokenResponse{
		TokenID:   tokenInfo.TokenID,
		AppName:   tokenInfo.AppName,
		Token:     tokenInfo.Token,
		CreatedAt: tokenInfo.CreatedAt,
		ExpiresAt: tokenInfo.ExpiresAt,
		ExpiresIn: int(tokenDuration.Seconds()),
	}

	h.logger.WithFields(logrus.Fields{
		"token_id":   tokenInfo.TokenID,
		"app_name":   tokenInfo.AppName,
		"expires_at": tokenInfo.ExpiresAt,
	}).Info("Auth: Token refreshed successfully")

	c.JSON(http.StatusOK, response)
}

// ExtendToken extends the expiration of an existing token
func (h *AuthHandler) ExtendToken(c *gin.Context) {
	var req ExtendTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Invalid request format",
			"details": err.Error(),
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"additional_hours": req.AdditionalHours,
		"remote_addr":      c.ClientIP(),
	}).Debug("Auth: Extending token")

	additionalDuration := time.Duration(req.AdditionalHours) * time.Hour

	// Extend token
	tokenInfo, err := h.jwtManager.ExtendToken(req.Token, additionalDuration)
	if err != nil {
		h.logger.WithError(err).Error("Failed to extend token")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":   "Failed to extend token",
			"details": err.Error(),
		})
		return
	}

	// Calculate remaining time
	remainingTime := time.Until(tokenInfo.ExpiresAt)
	if remainingTime < 0 {
		remainingTime = 0
	}

	response := CreateTokenResponse{
		TokenID:   tokenInfo.TokenID,
		AppName:   tokenInfo.AppName,
		Token:     tokenInfo.Token,
		CreatedAt: tokenInfo.CreatedAt,
		ExpiresAt: tokenInfo.ExpiresAt,
		ExpiresIn: int(remainingTime.Seconds()),
	}

	h.logger.WithFields(logrus.Fields{
		"token_id":       tokenInfo.TokenID,
		"app_name":       tokenInfo.AppName,
		"new_expires_at": tokenInfo.ExpiresAt,
	}).Info("Auth: Token extended successfully")

	c.JSON(http.StatusOK, response)
}

// GetTokenInfo returns information about the current token
func (h *AuthHandler) GetTokenInfo(c *gin.Context) {
	claims, exists := auth.GetClaims(c)
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Not authenticated",
		})
		return
	}

	// Calculate remaining time
	remainingTime := time.Until(time.Unix(claims.ExpiresAt, 0))
	if remainingTime < 0 {
		remainingTime = 0
	}

	response := VerifyTokenResponse{
		Valid:     true,
		TokenID:   claims.TokenID,
		AppName:   claims.AppName,
		CreatedAt: time.Unix(claims.CreatedAt, 0),
		ExpiresAt: time.Unix(claims.ExpiresAt, 0),
		ExpiresIn: int(remainingTime.Seconds()),
	}

	c.JSON(http.StatusOK, response)
}

// Logout handles token logout (client-side token invalidation)
func (h *AuthHandler) Logout(c *gin.Context) {
	claims, exists := auth.GetClaims(c)
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error": "Not authenticated",
		})
		return
	}

	h.logger.WithFields(logrus.Fields{
		"token_id":    claims.TokenID,
		"app_name":    claims.AppName,
		"remote_addr": c.ClientIP(),
	}).Info("Auth: Token logout")

	c.JSON(http.StatusOK, gin.H{
		"message":  "Logged out successfully",
		"token_id": claims.TokenID,
	})
}
