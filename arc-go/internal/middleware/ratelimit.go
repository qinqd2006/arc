package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

// RateLimiterConfig holds rate limiter configuration
type RateLimiterConfig struct {
	RequestsPerSecond float64       // Requests per second allowed
	Burst             int           // Maximum burst size
	CleanupInterval   time.Duration // How often to clean up old limiters
}

// RateLimiter provides per-IP rate limiting
type RateLimiter struct {
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	config   RateLimiterConfig
	logger   *logrus.Logger
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config RateLimiterConfig, logger *logrus.Logger) *RateLimiter {
	if logger == nil {
		logger = logrus.New()
	}

	rl := &RateLimiter{
		limiters: make(map[string]*rate.Limiter),
		config:   config,
		logger:   logger,
	}

	// Start cleanup goroutine
	go rl.cleanupLoop()

	return rl
}

// Middleware returns a Gin middleware handler for rate limiting
func (rl *RateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get client IP
		ip := c.ClientIP()

		// Get or create limiter for this IP
		limiter := rl.getLimiter(ip)

		// Check if request is allowed
		if !limiter.Allow() {
			rl.logger.WithFields(logrus.Fields{
				"ip":     ip,
				"path":   c.Request.URL.Path,
				"method": c.Request.Method,
			}).Warn("Rate limit exceeded")

			c.Header("X-RateLimit-Limit", formatRate(rl.config.RequestsPerSecond))
			c.Header("X-RateLimit-Remaining", "0")
			c.Header("Retry-After", "1")

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":   "Rate limit exceeded",
				"message": "Too many requests. Please slow down.",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// getLimiter gets or creates a limiter for an IP
func (rl *RateLimiter) getLimiter(ip string) *rate.Limiter {
	rl.mu.RLock()
	limiter, exists := rl.limiters[ip]
	rl.mu.RUnlock()

	if exists {
		return limiter
	}

	// Create new limiter
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists := rl.limiters[ip]; exists {
		return limiter
	}

	limiter = rate.NewLimiter(rate.Limit(rl.config.RequestsPerSecond), rl.config.Burst)
	rl.limiters[ip] = limiter

	return limiter
}

// cleanupLoop periodically removes inactive limiters
func (rl *RateLimiter) cleanupLoop() {
	interval := rl.config.CleanupInterval
	if interval == 0 {
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		rl.cleanup()
	}
}

// cleanup removes limiters that haven't been used recently
func (rl *RateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	before := len(rl.limiters)

	// Remove limiters with full token bucket (inactive)
	for ip, limiter := range rl.limiters {
		// If limiter has all tokens, it hasn't been used
		if limiter.Tokens() >= float64(rl.config.Burst) {
			delete(rl.limiters, ip)
		}
	}

	removed := before - len(rl.limiters)
	if removed > 0 {
		rl.logger.WithField("removed", removed).Debug("Cleaned up inactive rate limiters")
	}
}

// GetStats returns rate limiter statistics
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return map[string]interface{}{
		"active_limiters":     len(rl.limiters),
		"requests_per_second": rl.config.RequestsPerSecond,
		"burst_size":          rl.config.Burst,
	}
}

// formatRate formats requests per second for header
func formatRate(rps float64) string {
	if rps >= 1 {
		return formatInt(int(rps)) + "/s"
	}
	// Convert to requests per minute if less than 1/s
	return formatInt(int(rps*60)) + "/min"
}

func formatInt(n int) string {
	if n < 1000 {
		return string(rune(n + '0'))
	}
	return "1000+"
}

// PerTokenRateLimiter provides rate limiting per API token
type PerTokenRateLimiter struct {
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	config   RateLimiterConfig
	logger   *logrus.Logger
}

// NewPerTokenRateLimiter creates a rate limiter that limits per token
func NewPerTokenRateLimiter(config RateLimiterConfig, logger *logrus.Logger) *PerTokenRateLimiter {
	if logger == nil {
		logger = logrus.New()
	}

	rl := &PerTokenRateLimiter{
		limiters: make(map[string]*rate.Limiter),
		config:   config,
		logger:   logger,
	}

	go rl.cleanupLoop()

	return rl
}

// Middleware returns a Gin middleware for per-token rate limiting
func (rl *PerTokenRateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get token from header
		token := c.GetHeader("Authorization")
		if token == "" {
			token = c.GetHeader("X-API-Token")
		}

		// Use IP if no token
		if token == "" {
			token = "ip:" + c.ClientIP()
		}

		limiter := rl.getLimiter(token)

		if !limiter.Allow() {
			rl.logger.WithFields(logrus.Fields{
				"token": token[:min(len(token), 20)] + "...",
				"path":  c.Request.URL.Path,
			}).Warn("Token rate limit exceeded")

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error": "Rate limit exceeded for this API token",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

func (rl *PerTokenRateLimiter) getLimiter(token string) *rate.Limiter {
	rl.mu.RLock()
	limiter, exists := rl.limiters[token]
	rl.mu.RUnlock()

	if exists {
		return limiter
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	if limiter, exists := rl.limiters[token]; exists {
		return limiter
	}

	limiter = rate.NewLimiter(rate.Limit(rl.config.RequestsPerSecond), rl.config.Burst)
	rl.limiters[token] = limiter

	return limiter
}

func (rl *PerTokenRateLimiter) cleanupLoop() {
	interval := rl.config.CleanupInterval
	if interval == 0 {
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		rl.cleanup()
	}
}

func (rl *PerTokenRateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	for token, limiter := range rl.limiters {
		if limiter.Tokens() >= float64(rl.config.Burst) {
			delete(rl.limiters, token)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
