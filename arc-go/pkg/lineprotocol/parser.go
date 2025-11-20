package lineprotocol

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/sirupsen/logrus"
)

// Parser handles InfluxDB Line Protocol parsing
type Parser struct {
	pool          memory.Allocator
	logger        *logrus.Logger
	measurementRE *regexp.Regexp
	tagRE         *regexp.Regexp
	fieldRE       *regexp.Regexp
}

// Point represents a single data point from Line Protocol
type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Timestamp   time.Time
}

// NewParser creates a new Line Protocol parser
func NewParser(pool memory.Allocator, logger *logrus.Logger) *Parser {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	if logger == nil {
		logger = logrus.New()
	}

	return &Parser{
		pool:          pool,
		logger:        logger,
		measurementRE: regexp.MustCompile(`^[^,\s]+$`),
		tagRE:         regexp.MustCompile(`^([^=]+)=([^,\s]+)$`),
		fieldRE:       regexp.MustCompile(`^([^=]+)=([+-]?\d*\.?\d+(?:[eE][+-]?\d+)?[i]?)|([^=]+)="([^"]*)"$`),
	}
}

// ParseLine parses a single Line Protocol line
func (p *Parser) ParseLine(line string) (*Point, error) {
	start := time.Now()
	defer func() {
		p.logger.WithFields(logrus.Fields{
			"line_length": len(line),
			"took":        time.Since(start),
		}).Debug("LineProtocol: ParseLine completed")
	}()

	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty line")
	}

	// Split into components: measurement,tags fields timestamp
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid line format: %s", line)
	}

	measurementPart := parts[0]
	fieldsPart := parts[1]
	timestampPart := ""
	if len(parts) == 3 {
		timestampPart = parts[2]
	}

	// Parse measurement and tags
	measurement, tags, err := p.parseMeasurementTags(measurementPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse measurement/tags: %w", err)
	}

	// Parse fields
	fields, err := p.parseFields(fieldsPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse fields: %w", err)
	}

	// Parse timestamp
	timestamp, err := p.parseTimestamp(timestampPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	point := &Point{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Timestamp:   timestamp,
	}

	p.logger.WithFields(logrus.Fields{
		"measurement":  measurement,
		"tags_count":   len(tags),
		"fields_count": len(fields),
		"timestamp":    timestamp.UnixNano(),
	}).Debug("LineProtocol: Parsed point")

	return point, nil
}

// ParseLines parses multiple Line Protocol lines
func (p *Parser) ParseLines(lines string) ([]*Point, error) {
	start := time.Now()
	defer func() {
		p.logger.WithFields(logrus.Fields{
			"lines_count": strings.Count(lines, "\n") + 1,
			"took":        time.Since(start),
		}).Debug("LineProtocol: ParseLines completed")
	}()

	var points []*Point
	lines = strings.TrimSpace(lines)

	if lines == "" {
		return points, nil
	}

	// Split by lines
	lineList := strings.Split(lines, "\n")
	for i, line := range lineList {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		point, err := p.ParseLine(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse line %d: %w", i, err)
		}

		points = append(points, point)
	}

	return points, nil
}

// ToArrow converts points to Arrow Record
func (p *Parser) ToArrow(points []*Point) (arrow.Record, error) {
	if len(points) == 0 {
		return nil, fmt.Errorf("no points to convert")
	}

	start := time.Now()
	defer func() {
		p.logger.WithFields(logrus.Fields{
			"points_count": len(points),
			"took":         time.Since(start),
		}).Debug("LineProtocol: ToArrow completed")
	}()

	// Group points by measurement (assuming same measurement for now)
	measurement := points[0].Measurement
	for _, point := range points {
		if point.Measurement != measurement {
			return nil, fmt.Errorf("multiple measurements in batch: %s and %s", measurement, point.Measurement)
		}
	}

	// Collect all unique tags and fields
	allTags := make(map[string]bool)
	allFields := make(map[string]bool)

	for _, point := range points {
		for tag := range point.Tags {
			allTags[tag] = true
		}
		for field := range point.Fields {
			allFields[field] = true
		}
	}

	// Build schema
	fields := make([]arrow.Field, 0, len(allTags)+len(allFields)+2)

	// Add time field
	fields = append(fields, arrow.Field{
		Name: "time",
		Type: arrow.PrimitiveTypes.Int64,
	})

	// Add measurement field
	fields = append(fields, arrow.Field{
		Name: "measurement",
		Type: arrow.BinaryTypes.String,
	})

	// Add tag fields
	for tag := range allTags {
		fields = append(fields, arrow.Field{
			Name: tag,
			Type: arrow.BinaryTypes.String,
		})
	}

	// Add field fields
	for field := range allFields {
		dataType := p.inferFieldType(field, points)
		fields = append(fields, arrow.Field{
			Name: field,
			Type: dataType,
		})
	}

	schema := arrow.NewSchema(fields, nil)

	// Create arrays
	arrays := make([]arrow.Array, len(fields))
	fieldIndex := 0

	// Build time array
	timeBuilder := array.NewInt64Builder(p.pool)
	for _, point := range points {
		timeBuilder.Append(point.Timestamp.UnixNano())
	}
	arrays[fieldIndex] = timeBuilder.NewArray()
	timeBuilder.Release()
	fieldIndex++

	// Build measurement array
	measBuilder := array.NewStringBuilder(p.pool)
	for _, point := range points {
		measBuilder.Append(point.Measurement)
	}
	arrays[fieldIndex] = measBuilder.NewArray()
	measBuilder.Release()
	fieldIndex++

	// Build tag arrays
	for tag := range allTags {
		builder := array.NewStringBuilder(p.pool)
		for _, point := range points {
			if value, exists := point.Tags[tag]; exists {
				builder.Append(value)
			} else {
				builder.AppendNull()
			}
		}
		arrays[fieldIndex] = builder.NewArray()
		builder.Release()
		fieldIndex++
	}

	// Build field arrays
	for field := range allFields {
		dataType := p.inferFieldType(field, points)
		builder := p.createBuilder(dataType)
		if builder == nil {
			return nil, fmt.Errorf("unsupported data type for field %s: %v", field, dataType)
		}

		for _, point := range points {
			if value, exists := point.Fields[field]; exists {
				p.appendValue(builder, value)
			} else {
				builder.AppendNull()
			}
		}

		arrays[fieldIndex] = builder.NewArray()
		builder.Release()
		fieldIndex++
	}

	// Create record
	record := array.NewRecord(schema, arrays, int64(len(points)))

	p.logger.WithFields(logrus.Fields{
		"measurement":  measurement,
		"tags_count":   len(allTags),
		"fields_count": len(allFields),
		"rows":         len(points),
	}).Debug("LineProtocol: Built Arrow record")

	return record, nil
}

// parseMeasurementTags parses measurement and tags from the first part
func (p *Parser) parseMeasurementTags(part string) (string, map[string]string, error) {
	// Split measurement and tags
	parts := strings.Split(part, ",")
	if len(parts) == 0 {
		return "", nil, fmt.Errorf("empty measurement part")
	}

	measurement := p.escapeMeasurement(parts[0])
	if measurement == "" {
		return "", nil, fmt.Errorf("empty measurement")
	}

	tags := make(map[string]string)

	// Parse tags
	for i := 1; i < len(parts); i++ {
		tagPart := parts[i]
		matches := p.tagRE.FindStringSubmatch(tagPart)
		if len(matches) != 3 {
			continue // Skip invalid tag format
		}

		key := p.escapeKey(matches[1])
		value := p.escapeValue(matches[2])

		if key != "" && value != "" {
			tags[key] = value
		}
	}

	return measurement, tags, nil
}

// parseFields parses fields from the second part
func (p *Parser) parseFields(part string) (map[string]interface{}, error) {
	fields := make(map[string]string)
	fieldParts := strings.Split(part, ",")

	for _, fieldPart := range fieldParts {
		// Check if it's a string field
		if strings.Contains(fieldPart, "=\"") {
			// String field: key="value"
			parts := strings.SplitN(fieldPart, "=", 2)
			if len(parts) == 2 {
				key := p.escapeKey(parts[0])
				value := strings.Trim(parts[1], `"`)
				fields[key] = value
			}
		} else {
			// Numeric field: key=value
			parts := strings.SplitN(fieldPart, "=", 2)
			if len(parts) == 2 {
				key := p.escapeKey(parts[0])
				value := parts[1]
				fields[key] = value
			}
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no valid fields found")
	}

	// Convert string values to appropriate types
	converted := make(map[string]interface{})
	for key, value := range fields {
		convertedValue, err := p.convertFieldValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field value for %s: %w", key, err)
		}
		converted[key] = convertedValue
	}

	return converted, nil
}

// parseTimestamp parses timestamp from the third part
func (p *Parser) parseTimestamp(part string) (time.Time, error) {
	if part == "" {
		// Use current time if no timestamp provided
		return time.Now().UTC(), nil
	}

	// Parse nanosecond timestamp
	timestamp, err := strconv.ParseInt(part, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid timestamp: %s", part)
	}

	// Convert from nanoseconds to time
	return time.Unix(0, timestamp).UTC(), nil
}

// convertFieldValue converts field value string to appropriate type
func (p *Parser) convertFieldValue(value string) (interface{}, error) {
	// Check for boolean
	if value == "true" {
		return true, nil
	}
	if value == "false" {
		return false, nil
	}

	// Check for integer (ending with i)
	if strings.HasSuffix(value, "i") {
		intStr := strings.TrimSuffix(value, "i")
		if intValue, err := strconv.ParseInt(intStr, 10, 64); err == nil {
			return intValue, nil
		}
	}

	// Check for float
	if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
		return floatValue, nil
	}

	// Default to string
	return value, nil
}

// inferFieldType infers the Arrow data type for a field across all points
func (p *Parser) inferFieldType(field string, points []*Point) arrow.DataType {
	hasFloat := false
	hasInt := false
	hasBool := false

	for _, point := range points {
		if value, exists := point.Fields[field]; exists {
			switch value.(type) {
			case float64:
				hasFloat = true
			case int64:
				hasInt = true
			case bool:
				hasBool = true
			}
		}
	}

	// Prioritize: float > int > bool > string
	if hasFloat {
		return arrow.PrimitiveTypes.Float64
	}
	if hasInt {
		return arrow.PrimitiveTypes.Int64
	}
	if hasBool {
		return arrow.FixedWidthTypes.Boolean
	}
	return arrow.BinaryTypes.String
}

// createBuilder creates an appropriate array builder for the given data type
func (p *Parser) createBuilder(dataType arrow.DataType) array.Builder {
	switch dataType {
	case arrow.PrimitiveTypes.Int64:
		return array.NewInt64Builder(p.pool)
	case arrow.PrimitiveTypes.Float64:
		return array.NewFloat64Builder(p.pool)
	case arrow.BinaryTypes.String:
		return array.NewStringBuilder(p.pool)
	case arrow.FixedWidthTypes.Boolean:
		return array.NewBooleanBuilder(p.pool)
	default:
		return nil
	}
}

// appendValue appends a value to the appropriate builder
func (p *Parser) appendValue(builder array.Builder, value interface{}) {
	switch b := builder.(type) {
	case *array.Int64Builder:
		if v, ok := value.(int64); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.Float64Builder:
		if v, ok := value.(float64); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			b.Append(v)
		} else {
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// escapeMeasurement escapes measurement name
func (p *Parser) escapeMeasurement(measurement string) string {
	// Unescape spaces and commas
	measurement = strings.ReplaceAll(measurement, "\\ ", " ")
	measurement = strings.ReplaceAll(measurement, "\\,", ",")
	return measurement
}

// escapeKey escapes tag/field key
func (p *Parser) escapeKey(key string) string {
	// Unescape spaces, commas, and equals
	key = strings.ReplaceAll(key, "\\ ", " ")
	key = strings.ReplaceAll(key, "\\,", ",")
	key = strings.ReplaceAll(key, "\\=", "=")
	return key
}

// escapeValue escapes tag value
func (p *Parser) escapeValue(value string) string {
	// Unescape spaces and commas
	value = strings.ReplaceAll(value, "\\ ", " ")
	value = strings.ReplaceAll(value, "\\,", ",")
	return value
}

// ValidateLine validates a Line Protocol line without fully parsing it
func (p *Parser) ValidateLine(line string) error {
	line = strings.TrimSpace(line)
	if line == "" {
		return fmt.Errorf("empty line")
	}

	// Basic format check
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 2 {
		return fmt.Errorf("invalid line format: %s", line)
	}

	// Check measurement part
	if strings.Contains(parts[0], " ") && !strings.Contains(parts[0], "\\ ") {
		return fmt.Errorf("unescaped space in measurement/tags: %s", parts[0])
	}

	// Check fields part
	if !strings.Contains(parts[1], "=") {
		return fmt.Errorf("no fields found: %s", parts[1])
	}

	// Check timestamp format if present
	if len(parts) == 3 {
		if _, err := strconv.ParseInt(parts[2], 10, 64); err != nil {
			return fmt.Errorf("invalid timestamp: %s", parts[2])
		}
	}

	return nil
}
