package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// QueryCache provides caching for query results
type QueryCache struct {
	config  CacheConfig
	logger  *logrus.Logger
	mu      sync.RWMutex
	entries map[string]*CacheEntry
	metrics CacheMetrics

	// Cleanup
	stopCleanup chan struct{}
	wg          sync.WaitGroup
}

// CacheConfig holds cache configuration
type CacheConfig struct {
	Enabled         bool          `toml:"enabled"`
	MaxSizeMB       int           `toml:"max_size_mb"`
	TTLSeconds      int           `toml:"ttl_seconds"`
	CleanupInterval time.Duration `toml:"cleanup_interval"`
	MaxEntries      int           `toml:"max_entries"`
}

// CacheEntry represents a cached query result
type CacheEntry struct {
	Key        string      `json:"key"`
	Query      string      `json:"query"`
	Result     interface{} `json:"result"`
	Size       int64       `json:"size"`
	CreatedAt  time.Time   `json:"created_at"`
	ExpiresAt  time.Time   `json:"expires_at"`
	AccessedAt time.Time   `json:"accessed_at"`
	HitCount   int64       `json:"hit_count"`
}

// CacheMetrics holds cache performance metrics
type CacheMetrics struct {
	Hits         int64     `json:"hits"`
	Misses       int64     `json:"misses"`
	Evictions    int64     `json:"evictions"`
	Entries      int       `json:"entries"`
	SizeBytes    int64     `json:"size_bytes"`
	HitRate      float64   `json:"hit_rate"`
	LastCleanup  time.Time `json:"last_cleanup"`
	AvgQueryTime float64   `json:"avg_query_time_ms"`
}

// NewQueryCache creates a new query cache
func NewQueryCache(config CacheConfig, logger *logrus.Logger) *QueryCache {
	if logger == nil {
		logger = logrus.New()
	}

	cache := &QueryCache{
		config:      config,
		logger:      logger,
		entries:     make(map[string]*CacheEntry),
		metrics:     CacheMetrics{},
		stopCleanup: make(chan struct{}),
	}

	if config.Enabled {
		// Start cleanup goroutine
		cache.wg.Add(1)
		go cache.cleanupLoop()

		logger.WithFields(logrus.Fields{
			"max_size_mb":      config.MaxSizeMB,
			"ttl_seconds":      config.TTLSeconds,
			"cleanup_interval": config.CleanupInterval,
			"max_entries":      config.MaxEntries,
		}).Info("Query cache initialized")
	} else {
		logger.Info("Query cache is disabled")
	}

	return cache
}

// Get retrieves a cached query result
func (c *QueryCache) Get(ctx context.Context, query string, database string) (interface{}, bool) {
	if !c.config.Enabled {
		return nil, false
	}

	key := c.generateKey(query, database)

	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()

	if !exists {
		c.mu.Lock()
		c.metrics.Misses++
		c.mu.Unlock()
		return nil, false
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.metrics.Evictions++
		c.metrics.Entries = len(c.entries)
		c.metrics.Misses++
		c.mu.Unlock()
		return nil, false
	}

	// Update access time and hit count
	c.mu.Lock()
	entry.AccessedAt = time.Now()
	entry.HitCount++
	c.metrics.Hits++
	c.updateHitRate()
	c.mu.Unlock()

	c.logger.WithFields(logrus.Fields{
		"query":     query[:min(len(query), 50)],
		"database":  database,
		"hit_count": entry.HitCount,
	}).Debug("Cache hit")

	return entry.Result, true
}

// Set stores a query result in the cache
func (c *QueryCache) Set(ctx context.Context, query string, database string, result interface{}) error {
	if !c.config.Enabled {
		return nil
	}

	key := c.generateKey(query, database)

	// Calculate size
	size := c.estimateSize(result)

	// Check if we need to evict entries
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if max entries reached
	if len(c.entries) >= c.config.MaxEntries {
		c.evictLRU()
	}

	// Evict if max size reached
	maxSize := int64(c.config.MaxSizeMB) * 1024 * 1024
	for c.metrics.SizeBytes+size > maxSize && len(c.entries) > 0 {
		c.evictLRU()
	}

	entry := &CacheEntry{
		Key:        key,
		Query:      query,
		Result:     result,
		Size:       size,
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(time.Duration(c.config.TTLSeconds) * time.Second),
		AccessedAt: time.Now(),
		HitCount:   0,
	}

	c.entries[key] = entry
	c.metrics.SizeBytes += size
	c.metrics.Entries = len(c.entries)

	c.logger.WithFields(logrus.Fields{
		"query":    query[:min(len(query), 50)],
		"database": database,
		"size":     size,
	}).Debug("Cache set")

	return nil
}

// Invalidate removes a specific query from the cache
func (c *QueryCache) Invalidate(ctx context.Context, query string, database string) error {
	if !c.config.Enabled {
		return nil
	}

	key := c.generateKey(query, database)

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[key]
	if exists {
		c.metrics.SizeBytes -= entry.Size
		delete(c.entries, key)
		c.metrics.Entries = len(c.entries)
		c.metrics.Evictions++

		c.logger.WithFields(logrus.Fields{
			"query":    query[:min(len(query), 50)],
			"database": database,
		}).Debug("Cache invalidated")
	}

	return nil
}

// InvalidateDatabase removes all queries for a database
func (c *QueryCache) InvalidateDatabase(ctx context.Context, database string) error {
	if !c.config.Enabled {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	for key, entry := range c.entries {
		// Check if the query belongs to this database
		// This is a simplified approach; you might want a more sophisticated method
		if contains(entry.Query, database) {
			c.metrics.SizeBytes -= entry.Size
			delete(c.entries, key)
			count++
		}
	}

	c.metrics.Entries = len(c.entries)
	c.metrics.Evictions += int64(count)

	c.logger.WithFields(logrus.Fields{
		"database":        database,
		"entries_removed": count,
	}).Info("Database cache invalidated")

	return nil
}

// Clear removes all entries from the cache
func (c *QueryCache) Clear(ctx context.Context) error {
	if !c.config.Enabled {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	count := len(c.entries)
	c.entries = make(map[string]*CacheEntry)
	c.metrics.SizeBytes = 0
	c.metrics.Entries = 0
	c.metrics.Evictions += int64(count)

	c.logger.WithField("entries_removed", count).Info("Cache cleared")

	return nil
}

// GetMetrics returns cache metrics
func (c *QueryCache) GetMetrics() CacheMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := c.metrics
	return metrics
}

// ResetMetrics resets cache metrics
func (c *QueryCache) ResetMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics = CacheMetrics{
		Entries:   len(c.entries),
		SizeBytes: c.metrics.SizeBytes, // Keep current size
	}
}

// Close gracefully shuts down the cache
func (c *QueryCache) Close() error {
	c.logger.Info("Shutting down query cache")

	close(c.stopCleanup)
	c.wg.Wait()

	c.logger.Info("Query cache shutdown complete")
	return nil
}

// Helper methods

func (c *QueryCache) generateKey(query string, database string) string {
	data := fmt.Sprintf("%s:%s", database, query)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func (c *QueryCache) estimateSize(result interface{}) int64 {
	// Rough estimate using JSON marshaling
	data, err := json.Marshal(result)
	if err != nil {
		return 1024 // Default to 1KB if can't estimate
	}
	return int64(len(data))
}

func (c *QueryCache) evictLRU() {
	// Find least recently used entry
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.entries {
		if oldestKey == "" || entry.AccessedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.AccessedAt
		}
	}

	if oldestKey != "" {
		entry := c.entries[oldestKey]
		c.metrics.SizeBytes -= entry.Size
		delete(c.entries, oldestKey)
		c.metrics.Evictions++

		c.logger.WithFields(logrus.Fields{
			"key":   oldestKey,
			"query": entry.Query[:min(len(entry.Query), 50)],
		}).Debug("Evicted LRU entry")
	}
}

func (c *QueryCache) updateHitRate() {
	total := c.metrics.Hits + c.metrics.Misses
	if total > 0 {
		c.metrics.HitRate = float64(c.metrics.Hits) / float64(total)
	}
}

func (c *QueryCache) cleanupLoop() {
	defer c.wg.Done()

	interval := c.config.CleanupInterval
	if interval == 0 {
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.cleanup()
		case <-c.stopCleanup:
			return
		}
	}
}

func (c *QueryCache) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	count := 0

	for key, entry := range c.entries {
		if now.After(entry.ExpiresAt) {
			c.metrics.SizeBytes -= entry.Size
			delete(c.entries, key)
			count++
		}
	}

	c.metrics.Entries = len(c.entries)
	c.metrics.Evictions += int64(count)
	c.metrics.LastCleanup = now

	if count > 0 {
		c.logger.WithField("expired_entries", count).Debug("Cleaned up expired cache entries")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
