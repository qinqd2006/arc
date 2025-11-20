package logs

import (
	"container/ring"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
	Caller    string                 `json:"caller,omitempty"`
}

// LogManager manages in-memory log storage
type LogManager struct {
	buffer   *ring.Ring
	capacity int
	mu       sync.RWMutex
}

// NewLogManager creates a new log manager
func NewLogManager(capacity int) *LogManager {
	if capacity <= 0 {
		capacity = 1000 // Default capacity
	}

	return &LogManager{
		buffer:   ring.New(capacity),
		capacity: capacity,
	}
}

// AddEntry adds a log entry to the buffer
func (lm *LogManager) AddEntry(entry *LogEntry) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.buffer.Value = entry
	lm.buffer = lm.buffer.Next()
}

// GetLogs returns recent logs with optional filtering
func (lm *LogManager) GetLogs(level string, limit int, since *time.Time) []*LogEntry {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if limit <= 0 || limit > lm.capacity {
		limit = lm.capacity
	}

	var logs []*LogEntry
	lm.buffer.Do(func(v interface{}) {
		if v == nil {
			return
		}

		entry, ok := v.(*LogEntry)
		if !ok {
			return
		}

		// Filter by level
		if level != "" && entry.Level != level {
			return
		}

		// Filter by timestamp
		if since != nil && entry.Timestamp.Before(*since) {
			return
		}

		logs = append(logs, entry)
	})

	// Return most recent entries up to limit
	if len(logs) > limit {
		return logs[len(logs)-limit:]
	}

	return logs
}

// GetStats returns log statistics
func (lm *LogManager) GetStats() map[string]interface{} {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	counts := make(map[string]int)
	total := 0

	lm.buffer.Do(func(v interface{}) {
		if v == nil {
			return
		}

		entry, ok := v.(*LogEntry)
		if !ok {
			return
		}

		counts[entry.Level]++
		total++
	})

	return map[string]interface{}{
		"capacity":   lm.capacity,
		"total_logs": total,
		"by_level":   counts,
	}
}

// LogrusHook is a logrus hook that sends logs to LogManager
type LogrusHook struct {
	manager *LogManager
}

// NewLogrusHook creates a new logrus hook
func NewLogrusHook(manager *LogManager) *LogrusHook {
	return &LogrusHook{
		manager: manager,
	}
}

// Levels returns the log levels to hook
func (h *LogrusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Fire is called when a log event is triggered
func (h *LogrusHook) Fire(entry *logrus.Entry) error {
	logEntry := &LogEntry{
		Timestamp: entry.Time,
		Level:     entry.Level.String(),
		Message:   entry.Message,
		Fields:    make(map[string]interface{}),
	}

	// Copy fields
	for k, v := range entry.Data {
		logEntry.Fields[k] = v
	}

	// Add caller information if available
	if entry.HasCaller() {
		logEntry.Caller = fmt.Sprintf("%s:%d", entry.Caller.File, entry.Caller.Line)
	}

	h.manager.AddEntry(logEntry)
	return nil
}
