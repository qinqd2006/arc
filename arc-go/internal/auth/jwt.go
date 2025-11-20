package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// JWTManager manages JWT token generation and validation
type JWTManager struct {
	secretKey     string
	tokenDuration time.Duration
	logger        *logrus.Logger
}

// Claims represents JWT claims
type Claims struct {
	TokenID   string `json:"jti"`
	AppName   string `json:"app_name"`
	CreatedAt int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
	jwt.RegisteredClaims
}

// TokenInfo represents information about a token
type TokenInfo struct {
	TokenID   string    `json:"token_id"`
	AppName   string    `json:"app_name"`
	Token     string    `json:"token"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// NewJWTManager creates a new JWT manager
func NewJWTManager(secretKey string, tokenDuration time.Duration, logger *logrus.Logger) *JWTManager {
	if logger == nil {
		logger = logrus.New()
	}

	if tokenDuration <= 0 {
		tokenDuration = 24 * time.Hour // Default 24 hours
	}

	return &JWTManager{
		secretKey:     secretKey,
		tokenDuration: tokenDuration,
		logger:        logger,
	}
}

// GenerateToken generates a new JWT token
func (j *JWTManager) GenerateToken(appName, description string) (*TokenInfo, error) {
	if appName == "" {
		appName = "default"
	}

	tokenID := uuid.New().String()
	now := time.Now()
	expiresAt := now.Add(j.tokenDuration)

	claims := &Claims{
		TokenID:   tokenID,
		AppName:   appName,
		CreatedAt: now.Unix(),
		ExpiresAt: expiresAt.Unix(),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			Issuer:    "arc-core",
			Subject:   appName,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString([]byte(j.secretKey))
	if err != nil {
		j.logger.WithError(err).Error("Failed to sign JWT token")
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	tokenInfo := &TokenInfo{
		TokenID:   tokenID,
		AppName:   appName,
		Token:     tokenString,
		CreatedAt: now,
		ExpiresAt: expiresAt,
	}

	j.logger.WithFields(logrus.Fields{
		"token_id":   tokenID,
		"app_name":   appName,
		"expires_at": expiresAt,
	}).Info("JWT token generated")

	return tokenInfo, nil
}

// ValidateToken validates a JWT token and returns claims
func (j *JWTManager) ValidateToken(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(j.secretKey), nil
	})

	if err != nil {
		j.logger.WithError(err).Debug("JWT token validation failed")
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("invalid token claims")
	}

	// Check if token is expired
	if time.Now().Unix() > claims.ExpiresAt {
		return nil, fmt.Errorf("token has expired")
	}

	j.logger.WithFields(logrus.Fields{
		"token_id": claims.TokenID,
		"app_name": claims.AppName,
	}).Debug("JWT token validated successfully")

	return claims, nil
}

// RefreshToken generates a new token with extended expiration
func (j *JWTManager) RefreshToken(oldTokenString string) (*TokenInfo, error) {
	claims, err := j.ValidateToken(oldTokenString)
	if err != nil {
		return nil, fmt.Errorf("invalid token for refresh: %w", err)
	}

	// Generate new token with same app name
	return j.GenerateToken(claims.AppName, fmt.Sprintf("Refreshed from token %s", claims.TokenID))
}

// ExtendToken extends the expiration of an existing token
func (j *JWTManager) ExtendToken(tokenString string, additionalDuration time.Duration) (*TokenInfo, error) {
	claims, err := j.ValidateToken(tokenString)
	if err != nil {
		return nil, fmt.Errorf("invalid token for extension: %w", err)
	}

	// Create new token with extended expiration
	newExpiresAt := time.Unix(claims.ExpiresAt, 0).Add(additionalDuration)

	newClaims := &Claims{
		TokenID:   claims.TokenID,
		AppName:   claims.AppName,
		CreatedAt: claims.CreatedAt,
		ExpiresAt: newExpiresAt.Unix(),
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(newExpiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Unix(claims.CreatedAt, 0)),
			NotBefore: jwt.NewNumericDate(time.Unix(claims.CreatedAt, 0)),
			Issuer:    "arc-core",
			Subject:   claims.AppName,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, newClaims)
	newTokenString, err := token.SignedString([]byte(j.secretKey))
	if err != nil {
		return nil, fmt.Errorf("failed to extend token: %w", err)
	}

	tokenInfo := &TokenInfo{
		TokenID:   claims.TokenID,
		AppName:   claims.AppName,
		Token:     newTokenString,
		CreatedAt: time.Unix(claims.CreatedAt, 0),
		ExpiresAt: newExpiresAt,
	}

	j.logger.WithFields(logrus.Fields{
		"token_id":       claims.TokenID,
		"app_name":       claims.AppName,
		"old_expires_at": time.Unix(claims.ExpiresAt, 0),
		"new_expires_at": newExpiresAt,
	}).Info("JWT token extended")

	return tokenInfo, nil
}

// GetTokenDuration returns the configured token duration
func (j *JWTManager) GetTokenDuration() time.Duration {
	return j.tokenDuration
}

// SetSecretKey updates the secret key (use with caution)
func (j *JWTManager) SetSecretKey(secretKey string) {
	j.secretKey = secretKey
	j.logger.Info("JWT secret key updated")
}

// IsTokenExpired checks if a token is expired without full validation
func (j *JWTManager) IsTokenExpired(tokenString string) bool {
	claims, err := j.ValidateToken(tokenString)
	if err != nil {
		return true
	}

	return time.Now().Unix() > claims.ExpiresAt
}

// GetTokenRemainingTime returns the remaining time until token expiration
func (j *JWTManager) GetTokenRemainingTime(tokenString string) (time.Duration, error) {
	claims, err := j.ValidateToken(tokenString)
	if err != nil {
		return 0, err
	}

	expiresAt := time.Unix(claims.ExpiresAt, 0)
	remaining := time.Until(expiresAt)

	if remaining < 0 {
		return 0, fmt.Errorf("token has expired")
	}

	return remaining, nil
}

// ParseTokenUnsafe parses a token without validation (for debugging)
func (j *JWTManager) ParseTokenUnsafe(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte("dummy"), nil // Don't validate signature
	})

	if err != nil {
		return nil, fmt.Errorf("failed to parse token: %w", err)
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}

	return claims, nil
}
