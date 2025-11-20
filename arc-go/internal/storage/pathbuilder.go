package storage

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DefaultPathBuilder implements the PathBuilder interface
type DefaultPathBuilder struct{}

// NewDefaultPathBuilder creates a new default path builder
func NewDefaultPathBuilder() *DefaultPathBuilder {
	return &DefaultPathBuilder{}
}

// BuildMeasurementPath creates a path for a measurement
// Format: {database}/{measurement}/{year}/{month}/{day}/{hour}/{file}
func (pb *DefaultPathBuilder) BuildMeasurementPath(database, measurement string, timestamp time.Time) string {
	year, month, day := timestamp.Date()
	hour := timestamp.Hour()

	return fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d",
		database,
		measurement,
		year,
		month,
		day,
		hour)
}

// BuildHourlyPath creates a path for hourly partitioning
// Format: {database}/{measurement}/{year}/{month}/{day}/{hour}
func (pb *DefaultPathBuilder) BuildHourlyPath(database, measurement string, year, month, day, hour int) string {
	return fmt.Sprintf("%s/%s/%04d/%02d/%02d/%02d",
		database,
		measurement,
		year,
		month,
		day,
		hour)
}

// BuildWALPath creates a path for WAL files
// Format: {database}/_wal/{workerID}-{timestamp}.wal
func (pb *DefaultPathBuilder) BuildWALPath(database, workerID string) string {
	timestamp := time.Now().Unix()
	return fmt.Sprintf("%s/_wal/%s-%d.wal", database, workerID, timestamp)
}

// BuildCompactionPath creates a path for compaction temp files
// Format: {database}/_compaction/{measurement}/{timestamp}-{workerID}/
func (pb *DefaultPathBuilder) BuildCompactionPath(database, measurement string, timestamp time.Time) string {
	unixTs := timestamp.Unix()
	return fmt.Sprintf("%s/_compaction/%s/%d", database, measurement, unixTs)
}

// BuildParquetFileName creates a filename for Parquet files
// Format: {measurement}-{timestamp}-{nanos}-{uuid}.parquet
func (pb *DefaultPathBuilder) BuildParquetFileName(measurement string, timestamp time.Time) string {
	nanos := timestamp.Nanosecond()
	unixTs := timestamp.Unix()

	return fmt.Sprintf("%s-%d-%09d.parquet", measurement, unixTs, nanos)
}

// ParsePath parses a path into components
func (pb *DefaultPathBuilder) ParsePath(path string) (database, measurement string, year, month, day, hour int, err error) {
	// Normalize path separators
	path = filepath.ToSlash(path)

	// Split path components
	parts := strings.Split(path, "/")
	if len(parts) < 6 {
		return "", "", 0, 0, 0, 0, fmt.Errorf("invalid path format: %s", path)
	}

	database = parts[0]
	measurement = parts[1]

	// Parse year, month, day, hour
	if year, err = strconv.Atoi(parts[2]); err != nil {
		return "", "", 0, 0, 0, 0, fmt.Errorf("invalid year: %s", parts[2])
	}
	if month, err = strconv.Atoi(parts[3]); err != nil {
		return "", "", 0, 0, 0, 0, fmt.Errorf("invalid month: %s", parts[3])
	}
	if day, err = strconv.Atoi(parts[4]); err != nil {
		return "", "", 0, 0, 0, 0, fmt.Errorf("invalid day: %s", parts[4])
	}
	if hour, err = strconv.Atoi(parts[5]); err != nil {
		return "", "", 0, 0, 0, 0, fmt.Errorf("invalid hour: %s", parts[5])
	}

	return database, measurement, year, month, day, hour, nil
}

// IsSystemPath checks if a path is a system path (WAL, compaction, etc.)
func (pb *DefaultPathBuilder) IsSystemPath(path string) bool {
	// Normalize path separators
	path = filepath.ToSlash(path)

	// Check for system directories
	systemDirs := []string{
		"/_wal/",
		"/_compaction/",
		"/_tmp/",
		"/_system/",
	}

	for _, dir := range systemDirs {
		if strings.Contains(path, dir) {
			return true
		}
	}

	return false
}

// ValidateMeasurement validates a measurement name
func (pb *DefaultPathBuilder) ValidateMeasurement(measurement string) error {
	if measurement == "" {
		return fmt.Errorf("measurement name cannot be empty")
	}

	// Check for invalid characters
	if strings.ContainsAny(measurement, "/\\:*?\"<>|") {
		return fmt.Errorf("measurement name contains invalid characters: %s", measurement)
	}

	// Check length
	if len(measurement) > 255 {
		return fmt.Errorf("measurement name too long (max 255 characters): %s", measurement)
	}

	// Check for system prefixes
	if strings.HasPrefix(measurement, "_") {
		return fmt.Errorf("measurement name cannot start with underscore: %s", measurement)
	}

	return nil
}

// ValidateDatabase validates a database name
func (pb *DefaultPathBuilder) ValidateDatabase(database string) error {
	if database == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	// Check for invalid characters
	if strings.ContainsAny(database, "/\\:*?\"<>|") {
		return fmt.Errorf("database name contains invalid characters: %s", database)
	}

	// Check length
	if len(database) > 64 {
		return fmt.Errorf("database name too long (max 64 characters): %s", database)
	}

	// Check for system prefixes
	if strings.HasPrefix(database, "_") {
		return fmt.Errorf("database name cannot start with underscore: %s", database)
	}

	return nil
}

// BuildHourPrefix returns the hour-level prefix for a timestamp
func (pb *DefaultPathBuilder) BuildHourPrefix(database, measurement string, timestamp time.Time) string {
	return pb.BuildMeasurementPath(database, measurement, timestamp)
}

// BuildDayPrefix returns the day-level prefix for a timestamp
func (pb *DefaultPathBuilder) BuildDayPrefix(database, measurement string, timestamp time.Time) string {
	year, month, day := timestamp.Date()
	return fmt.Sprintf("%s/%s/%04d/%02d/%02d", database, measurement, year, month, day)
}

// BuildMonthPrefix returns the month-level prefix for a timestamp
func (pb *DefaultPathBuilder) BuildMonthPrefix(database, measurement string, timestamp time.Time) string {
	year, month, _ := timestamp.Date()
	return fmt.Sprintf("%s/%s/%04d/%02d", database, measurement, year, month)
}

// BuildMeasurementPrefix returns the measurement-level prefix
func (pb *DefaultPathBuilder) BuildMeasurementPrefix(database, measurement string) string {
	return fmt.Sprintf("%s/%s", database, measurement)
}

// BuildDatabasePrefix returns the database-level prefix
func (pb *DefaultPathBuilder) BuildDatabasePrefix(database string) string {
	return database
}

// ParseTimeFromPath extracts the time from a path
func (pb *DefaultPathBuilder) ParseTimeFromPath(path string) (time.Time, error) {
	_, _, year, month, day, hour, err := pb.ParsePath(path)
	if err != nil {
		return time.Time{}, err
	}

	return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC), nil
}

// GetHourRange returns the start and end time for an hour
func (pb *DefaultPathBuilder) GetHourRange(timestamp time.Time) (time.Time, time.Time) {
	start := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), 0, 0, 0, timestamp.Location())
	end := start.Add(time.Hour)
	return start, end
}

// GetDayRange returns the start and end time for a day
func (pb *DefaultPathBuilder) GetDayRange(timestamp time.Time) (time.Time, time.Time) {
	start := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location())
	end := start.AddDate(0, 0, 1)
	return start, end
}

// GetMonthRange returns the start and end time for a month
func (pb *DefaultPathBuilder) GetMonthRange(timestamp time.Time) (time.Time, time.Time) {
	start := time.Date(timestamp.Year(), timestamp.Month(), 1, 0, 0, 0, 0, timestamp.Location())
	end := start.AddDate(0, 1, 0)
	return start, end
}

// SanitizePath sanitizes a path component
func (pb *DefaultPathBuilder) SanitizePath(component string) string {
	// Replace invalid characters with underscores
	re := regexp.MustCompile(`[/\\:*?"<>|]`)
	sanitized := re.ReplaceAllString(component, "_")

	// Remove leading and trailing underscores
	sanitized = strings.Trim(sanitized, "_")

	// Ensure it's not empty
	if sanitized == "" {
		sanitized = "unnamed"
	}

	return sanitized
}

// GetParentPath returns the parent directory path
func (pb *DefaultPathBuilder) GetParentPath(path string) string {
	return filepath.Dir(path)
}

// IsMeasurementPath checks if a path is a measurement path
func (pb *DefaultPathBuilder) IsMeasurementPath(path string) bool {
	// Normalize path separators
	path = filepath.ToSlash(path)

	// Split path components
	parts := strings.Split(path, "/")

	// Should have at least: database/measurement/year/month/day/hour
	return len(parts) >= 6
}
