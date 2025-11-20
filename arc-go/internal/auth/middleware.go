package auth

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// Middleware provides authentication middleware for Gin
type Middleware struct {
	jwtManager   *JWTManager
	defaultToken string
	allowlist    []string
	logger       *logrus.Logger
}

// NewMiddleware creates a new authentication middleware
func NewMiddleware(defaultToken string, allowlist []string, logger *logrus.Logger) *Middleware {
	if logger == nil {
		logger = logrus.New()
	}

	// Create JWT manager with default secret
	jwtManager := NewJWTManager(defaultToken, 24*time.Hour, logger)

	return &Middleware{
		jwtManager:   jwtManager,
		defaultToken: defaultToken,
		allowlist:    allowlist,
		logger:       logger,
	}
}

// Middleware returns the Gin middleware function
func (m *Middleware) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip authentication for allowlist paths
		if m.isAllowlisted(c.Request.URL.Path) {
			c.Next()
			return
		}

		// Get token from Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			// Try query parameter
			authHeader = c.Query("token")
		}

		if authHeader == "" {
			m.unauthorized(c, "Missing authorization token")
			return
		}

		// Remove "Bearer " prefix if present
		token := strings.TrimPrefix(authHeader, "Bearer ")
		token = strings.TrimSpace(token)

		// Validate token
		claims, err := m.validateToken(token)
		if err != nil {
			m.unauthorized(c, "Invalid or expired token")
			return
		}

		// Add claims to context
		c.Set("token_claims", claims)
		c.Set("app_name", claims.AppName)
		c.Set("token_id", claims.TokenID)

		m.logger.WithFields(logrus.Fields{
			"app_name":    claims.AppName,
			"token_id":    claims.TokenID,
			"path":        c.Request.URL.Path,
			"method":      c.Request.Method,
			"remote_addr": c.ClientIP(),
		}).Debug("Request authenticated")

		c.Next()
	}
}

// isAllowlisted checks if a path is in the allowlist
func (m *Middleware) isAllowlisted(path string) bool {
	for _, allowlistedPath := range m.allowlist {
		if strings.HasPrefix(path, allowlistedPath) {
			return true
		}
	}
	return false
}

// validateToken validates a token against both default token and JWT
func (m *Middleware) validateToken(token string) (*Claims, error) {
	// Check default token first (for backward compatibility)
	if m.defaultToken != "" && token == m.defaultToken {
		// Create claims for default token
		return &Claims{
			TokenID:   "default-token",
			AppName:   "default",
			CreatedAt: time.Now().Unix(),
			ExpiresAt: time.Now().Add(24 * time.Hour).Unix(),
		}, nil
	}

	// Try JWT validation
	return m.jwtManager.ValidateToken(token)
}

// unauthorized sends an unauthorized response
func (m *Middleware) unauthorized(c *gin.Context, message string) {
	c.JSON(http.StatusUnauthorized, gin.H{
		"error":   "Unauthorized",
		"message": message,
	})
	c.Abort()
}

// RequireAuth is a shortcut to require authentication for a route group
func (m *Middleware) RequireAuth() gin.HandlerFunc {
	return m.Middleware()
}

// OptionalAuth provides optional authentication (doesn't fail if no token)
func (m *Middleware) OptionalAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Skip authentication for allowlist paths
		if m.isAllowlisted(c.Request.URL.Path) {
			c.Next()
			return
		}

		// Try to get token
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			authHeader = c.Query("token")
		}

		if authHeader != "" {
			// Remove "Bearer " prefix if present
			token := strings.TrimPrefix(authHeader, "Bearer ")
			token = strings.TrimSpace(token)

			// Try validation (but don't fail if invalid)
			if claims, err := m.validateToken(token); err == nil {
				c.Set("token_claims", claims)
				c.Set("app_name", claims.AppName)
				c.Set("token_id", claims.TokenID)

				m.logger.WithFields(logrus.Fields{
					"app_name": claims.AppName,
					"token_id": claims.TokenID,
					"path":     c.Request.URL.Path,
					"method":   c.Request.Method,
				}).Debug("Optional authentication successful")
			}
		}

		c.Next()
	}
}

// RequireAppName creates middleware that requires a specific app name
func (m *Middleware) RequireAppName(appName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, exists := c.Get("token_claims")
		if !exists {
			m.unauthorized(c, "Authentication required")
			return
		}

		tokenClaims, ok := claims.(*Claims)
		if !ok {
			m.unauthorized(c, "Invalid token claims")
			return
		}

		if tokenClaims.AppName != appName {
			m.unauthorized(c, "Insufficient privileges")
			return
		}

		c.Next()
	}
}

// GetClaims extracts token claims from the context
func GetClaims(c *gin.Context) (*Claims, bool) {
	claims, exists := c.Get("token_claims")
	if !exists {
		return nil, false
	}

	tokenClaims, ok := claims.(*Claims)
	return tokenClaims, ok
}

// GetAppName extracts the app name from the context
func GetAppName(c *gin.Context) (string, bool) {
	appName, exists := c.Get("app_name")
	if !exists {
		return "", false
	}

	name, ok := appName.(string)
	return name, ok
}

// GetTokenID extracts the token ID from the context
func GetTokenID(c *gin.Context) (string, bool) {
	tokenID, exists := c.Get("token_id")
	if !exists {
		return "", false
	}

	id, ok := tokenID.(string)
	return id, ok
}

// IsAuthenticated checks if the request is authenticated
func IsAuthenticated(c *gin.Context) bool {
	_, exists := c.Get("token_claims")
	return exists
}
