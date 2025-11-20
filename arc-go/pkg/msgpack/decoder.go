package msgpack

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

// Decoder handles MessagePack decoding to Arrow format
type Decoder struct {
	pool   memory.Allocator
	logger *logrus.Logger
}

// MessageData represents the structure of incoming MessagePack data
type MessageData struct {
	Measurement string                 `msgpack:"m"`
	Columns     map[string]interface{} `msgpack:"columns"`
	Tags        map[string]string      `msgpack:"tags,omitempty"`
	Timestamp   int64                  `msgpack:"timestamp,omitempty"`
}

// NewDecoder creates a new MessagePack decoder
func NewDecoder(pool memory.Allocator, logger *logrus.Logger) *Decoder {
	if pool == nil {
		pool = memory.NewGoAllocator()
	}
	if logger == nil {
		logger = logrus.New()
	}

	return &Decoder{
		pool:   pool,
		logger: logger,
	}
}

// DecodeToArrow decodes MessagePack data to Arrow Record
func (d *Decoder) DecodeToArrow(data []byte) (arrow.Record, error) {
	start := time.Now()
	defer func() {
		d.logger.WithFields(logrus.Fields{
			"size": len(data),
			"took": time.Since(start),
		}).Debug("MessagePack: DecodeToArrow completed")
	}()

	// Decode MessagePack
	var msgData MessageData
	if err := msgpack.Unmarshal(data, &msgData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal MessagePack: %w", err)
	}

	// Validate data
	if err := d.validateMessageData(&msgData); err != nil {
		return nil, fmt.Errorf("invalid message data: %w", err)
	}

	// Convert to Arrow Record
	return d.buildArrowRecord(&msgData)
}

// DecodeBatch decodes multiple MessagePack messages to a single Arrow Record
func (d *Decoder) DecodeBatch(messages [][]byte) (arrow.Record, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to decode")
	}

	start := time.Now()
	defer func() {
		d.logger.WithFields(logrus.Fields{
			"count": len(messages),
			"size":  d.totalSize(messages),
			"took":  time.Since(start),
		}).Debug("MessagePack: DecodeBatch completed")
	}()

	// Decode all messages
	decoded := make([]*MessageData, 0, len(messages))
	for i, msg := range messages {
		var msgData MessageData
		if err := msgpack.Unmarshal(msg, &msgData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal message %d: %w", i, err)
		}

		if err := d.validateMessageData(&msgData); err != nil {
			return nil, fmt.Errorf("invalid message data in message %d: %w", i, err)
		}

		decoded = append(decoded, &msgData)
	}

	// Merge and convert to Arrow Record
	return d.mergeToArrowRecord(decoded)
}

// validateMessageData validates the MessageData structure
func (d *Decoder) validateMessageData(data *MessageData) error {
	if data.Measurement == "" {
		return fmt.Errorf("measurement name is required")
	}

	if len(data.Columns) == 0 {
		return fmt.Errorf("at least one column is required")
	}

	// Validate column data types
	for colName, colData := range data.Columns {
		if err := d.validateColumnData(colName, colData); err != nil {
			return fmt.Errorf("invalid column data for %s: %w", colName, err)
		}
	}

	return nil
}

// validateColumnData validates individual column data
func (d *Decoder) validateColumnData(name string, data interface{}) error {
	// Column data should be a slice
	switch v := data.(type) {
	case []interface{}:
		// Check if all elements are of the same type
		if len(v) > 0 {
			firstType := d.getValueType(v[0])
			for i, val := range v {
				if d.getValueType(val) != firstType {
					return fmt.Errorf("mixed types in column %s at index %d", name, i)
				}
			}
		}
		return nil
	case []int64, []int, []int32, []int16, []int8:
		return nil
	case []uint64, []uint, []uint32, []uint16, []uint8:
		return nil
	case []float64, []float32:
		return nil
	case []string:
		return nil
	case []bool:
		return nil
	default:
		return fmt.Errorf("column %s must be a slice, got %T", name, data)
	}
}

// getValueType determines the Arrow data type for a value
func (d *Decoder) getValueType(value interface{}) arrow.DataType {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return arrow.PrimitiveTypes.Int64
	case uint, uint8, uint16, uint32, uint64:
		return arrow.PrimitiveTypes.Uint64
	case float32, float64:
		return arrow.PrimitiveTypes.Float64
	case string:
		return arrow.BinaryTypes.String
	case bool:
		return arrow.FixedWidthTypes.Boolean
	default:
		return arrow.BinaryTypes.String
	}
}

// getSliceLength returns the length of any slice type
func (d *Decoder) getSliceLength(data interface{}) int {
	switch v := data.(type) {
	case []interface{}:
		return len(v)
	case []int64:
		return len(v)
	case []int:
		return len(v)
	case []int32:
		return len(v)
	case []int16:
		return len(v)
	case []int8:
		return len(v)
	case []uint64:
		return len(v)
	case []uint:
		return len(v)
	case []uint32:
		return len(v)
	case []uint16:
		return len(v)
	case []uint8:
		return len(v)
	case []float64:
		return len(v)
	case []float32:
		return len(v)
	case []string:
		return len(v)
	case []bool:
		return len(v)
	default:
		return 0
	}
}

// getSliceType returns the Arrow data type for a slice
func (d *Decoder) getSliceType(data interface{}) arrow.DataType {
	switch data.(type) {
	case []interface{}:
		// Need to check first element
		if slice := data.([]interface{}); len(slice) > 0 {
			return d.getValueType(slice[0])
		}
		return arrow.BinaryTypes.String
	case []int64, []int, []int32, []int16, []int8:
		return arrow.PrimitiveTypes.Int64
	case []uint64, []uint, []uint32, []uint16, []uint8:
		return arrow.PrimitiveTypes.Uint64
	case []float64, []float32:
		return arrow.PrimitiveTypes.Float64
	case []string:
		return arrow.BinaryTypes.String
	case []bool:
		return arrow.FixedWidthTypes.Boolean
	default:
		return arrow.BinaryTypes.String
	}
}

// buildArrowRecord builds an Arrow Record from MessageData
func (d *Decoder) buildArrowRecord(data *MessageData) (arrow.Record, error) {
	// Determine number of rows first
	numRows := 0
	for _, colData := range data.Columns {
		colLen := d.getSliceLength(colData)
		if colLen > numRows {
			numRows = colLen
		}
	}

	// Build schema with consistent ordering
	fields := make([]arrow.Field, 0, len(data.Columns)+2)

	// Track column names in order for array building
	columnOrder := make([]string, 0, len(data.Columns)+2)

	// Add time field if not present
	if _, hasTime := data.Columns["time"]; !hasTime && data.Timestamp > 0 {
		fields = append(fields, arrow.Field{
			Name: "time",
			Type: arrow.PrimitiveTypes.Int64,
		})
		columnOrder = append(columnOrder, "time")
	}

	// Add tag fields (sorted for consistency)
	tagNames := make([]string, 0, len(data.Tags))
	for tagName := range data.Tags {
		tagNames = append(tagNames, tagName)
	}
	sort.Strings(tagNames)

	for _, tagName := range tagNames {
		fields = append(fields, arrow.Field{
			Name: tagName,
			Type: arrow.BinaryTypes.String,
		})
		columnOrder = append(columnOrder, "tag:"+tagName)
	}

	// Add value fields (sorted for consistency)
	colNames := make([]string, 0, len(data.Columns))
	for colName := range data.Columns {
		if colName == "time" || colName == "timestamp" {
			continue
		}
		colData := data.Columns[colName]
		colLen := d.getSliceLength(colData)
		if colLen > 0 {
			colNames = append(colNames, colName)
		}
	}
	sort.Strings(colNames)

	for _, colName := range colNames {
		colData := data.Columns[colName]
		dataType := d.getSliceType(colData)
		fields = append(fields, arrow.Field{
			Name: colName,
			Type: dataType,
		})
		columnOrder = append(columnOrder, colName)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no valid fields found")
	}

	// Store measurement in schema metadata
	metadata := arrow.NewMetadata(
		[]string{"measurement"},
		[]string{data.Measurement},
	)
	schema := arrow.NewSchema(fields, &metadata)

	// Create arrays in the same order as fields
	arrays := make([]arrow.Array, len(fields))

	for i, colKey := range columnOrder {
		if colKey == "time" {
			// Add synthetic time array
			builder := array.NewInt64Builder(d.pool)
			for j := 0; j < numRows; j++ {
				builder.Append(data.Timestamp)
			}
			arrays[i] = builder.NewArray()
			builder.Release()
		} else if strings.HasPrefix(colKey, "tag:") {
			// Add tag array
			tagName := strings.TrimPrefix(colKey, "tag:")
			tagValue := data.Tags[tagName]
			builder := array.NewStringBuilder(d.pool)
			for j := 0; j < numRows; j++ {
				builder.Append(tagValue)
			}
			arrays[i] = builder.NewArray()
			builder.Release()
		} else {
			// Add value array
			colData := data.Columns[colKey]
			dataType := d.getSliceType(colData)
			builder := d.createBuilder(dataType)
			if builder == nil {
				return nil, fmt.Errorf("unsupported data type for column %s: %v", colKey, dataType)
			}

			// Append values from typed slices
			if err := d.appendSliceToBuilder(builder, colData); err != nil {
				builder.Release()
				return nil, fmt.Errorf("failed to append column %s: %w", colKey, err)
			}

			arrays[i] = builder.NewArray()
			builder.Release()
		}
	}

	// Create record
	record := array.NewRecord(schema, arrays, int64(numRows))

	d.logger.WithFields(logrus.Fields{
		"measurement": data.Measurement,
		"fields":      len(fields),
		"rows":        numRows,
	}).Debug("MessagePack: Built Arrow record")

	return record, nil
}

// mergeToArrowRecord merges multiple MessageData into a single Arrow Record
func (d *Decoder) mergeToArrowRecord(messages []*MessageData) (arrow.Record, error) {
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to merge")
	}

	// Group messages by measurement
	byMeasurement := make(map[string][]*MessageData)
	for _, msg := range messages {
		byMeasurement[msg.Measurement] = append(byMeasurement[msg.Measurement], msg)
	}

	// If all messages have the same measurement, merge them directly
	if len(byMeasurement) == 1 {
		measurement := messages[0].Measurement
		return d.mergeSameMeasurement(byMeasurement[measurement], measurement)
	}

	// Multiple measurements: merge each group separately and combine
	// For now, we'll just take the largest group
	// A more sophisticated approach would create multiple records
	var largestGroup []*MessageData
	var largestMeasurement string
	maxCount := 0

	for measurement, group := range byMeasurement {
		if len(group) > maxCount {
			maxCount = len(group)
			largestGroup = group
			largestMeasurement = measurement
		}
	}

	d.logger.WithFields(logrus.Fields{
		"total_measurements":   len(byMeasurement),
		"selected_measurement": largestMeasurement,
		"selected_count":       len(largestGroup),
	}).Debug("Multiple measurements detected, selecting largest group")

	return d.mergeSameMeasurement(largestGroup, largestMeasurement)
}

// mergeSameMeasurement merges messages with the same measurement
func (d *Decoder) mergeSameMeasurement(messages []*MessageData, measurement string) (arrow.Record, error) {
	// Collect all columns
	allColumns := make(map[string]interface{})
	for _, msg := range messages {
		for colName, colData := range msg.Columns {
			if colSlice, ok := colData.([]interface{}); ok {
				if allColumns[colName] == nil {
					allColumns[colName] = make([]interface{}, 0)
				}
				allColumns[colName] = append(allColumns[colName].([]interface{}), colSlice...)
			}
		}
	}

	// Create merged MessageData
	merged := &MessageData{
		Measurement: measurement,
		Columns:     allColumns,
		Tags:        messages[0].Tags, // Use tags from first message
		Timestamp:   messages[0].Timestamp,
	}

	return d.buildArrowRecord(merged)
}

// createBuilder creates an appropriate array builder for the given data type
func (d *Decoder) createBuilder(dataType arrow.DataType) array.Builder {
	switch dataType {
	case arrow.PrimitiveTypes.Int64:
		return array.NewInt64Builder(d.pool)
	case arrow.PrimitiveTypes.Uint64:
		return array.NewUint64Builder(d.pool)
	case arrow.PrimitiveTypes.Float64:
		return array.NewFloat64Builder(d.pool)
	case arrow.BinaryTypes.String:
		return array.NewStringBuilder(d.pool)
	case arrow.FixedWidthTypes.Boolean:
		return array.NewBooleanBuilder(d.pool)
	default:
		return nil
	}
}

// appendValue appends a value to the appropriate builder
func (d *Decoder) appendValue(builder array.Builder, value interface{}) {
	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := value.(type) {
		case int:
			b.Append(int64(v))
		case int8:
			b.Append(int64(v))
		case int16:
			b.Append(int64(v))
		case int32:
			b.Append(int64(v))
		case int64:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.Uint64Builder:
		switch v := value.(type) {
		case uint:
			b.Append(uint64(v))
		case uint8:
			b.Append(uint64(v))
		case uint16:
			b.Append(uint64(v))
		case uint32:
			b.Append(uint64(v))
		case uint64:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float32:
			b.Append(float64(v))
		case float64:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		if s, ok := value.(string); ok {
			b.Append(s)
		} else {
			b.AppendNull()
		}
	case *array.BooleanBuilder:
		if bl, ok := value.(bool); ok {
			b.Append(bl)
		} else {
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// appendSliceToBuilder appends a typed slice to the appropriate builder
func (d *Decoder) appendSliceToBuilder(builder array.Builder, data interface{}) error {
	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := data.(type) {
		case []int64:
			for _, val := range v {
				b.Append(val)
			}
		case []int:
			for _, val := range v {
				b.Append(int64(val))
			}
		case []int32:
			for _, val := range v {
				b.Append(int64(val))
			}
		case []int16:
			for _, val := range v {
				b.Append(int64(val))
			}
		case []int8:
			for _, val := range v {
				b.Append(int64(val))
			}
		case []interface{}:
			for _, val := range v {
				d.appendValue(b, val)
			}
		default:
			return fmt.Errorf("cannot append %T to Int64Builder", data)
		}
	case *array.Uint64Builder:
		switch v := data.(type) {
		case []uint64:
			for _, val := range v {
				b.Append(val)
			}
		case []uint:
			for _, val := range v {
				b.Append(uint64(val))
			}
		case []uint32:
			for _, val := range v {
				b.Append(uint64(val))
			}
		case []uint16:
			for _, val := range v {
				b.Append(uint64(val))
			}
		case []uint8:
			for _, val := range v {
				b.Append(uint64(val))
			}
		case []interface{}:
			for _, val := range v {
				d.appendValue(b, val)
			}
		default:
			return fmt.Errorf("cannot append %T to Uint64Builder", data)
		}
	case *array.Float64Builder:
		switch v := data.(type) {
		case []float64:
			for _, val := range v {
				b.Append(val)
			}
		case []float32:
			for _, val := range v {
				b.Append(float64(val))
			}
		case []interface{}:
			for _, val := range v {
				d.appendValue(b, val)
			}
		default:
			return fmt.Errorf("cannot append %T to Float64Builder", data)
		}
	case *array.StringBuilder:
		switch v := data.(type) {
		case []string:
			for _, val := range v {
				b.Append(val)
			}
		case []interface{}:
			for _, val := range v {
				d.appendValue(b, val)
			}
		default:
			return fmt.Errorf("cannot append %T to StringBuilder", data)
		}
	case *array.BooleanBuilder:
		switch v := data.(type) {
		case []bool:
			for _, val := range v {
				b.Append(val)
			}
		case []interface{}:
			for _, val := range v {
				d.appendValue(b, val)
			}
		default:
			return fmt.Errorf("cannot append %T to BooleanBuilder", data)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

// totalSize calculates the total size of a batch of messages
func (d *Decoder) totalSize(messages [][]byte) int {
	total := 0
	for _, msg := range messages {
		total += len(msg)
	}
	return total
}

// GetSchema extracts schema from MessagePack data without fully decoding
func (d *Decoder) GetSchema(data []byte) (*arrow.Schema, error) {
	var msgData MessageData
	if err := msgpack.Unmarshal(data, &msgData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal MessagePack: %w", err)
	}

	fields := make([]arrow.Field, 0, len(msgData.Columns)+2)

	// Add time field
	if _, hasTime := msgData.Columns["time"]; !hasTime && msgData.Timestamp > 0 {
		fields = append(fields, arrow.Field{
			Name: "time",
			Type: arrow.PrimitiveTypes.Int64,
		})
	}

	// Add tag fields
	for tagName := range msgData.Tags {
		fields = append(fields, arrow.Field{
			Name: tagName,
			Type: arrow.BinaryTypes.String,
		})
	}

	// Add value fields
	for colName, colData := range msgData.Columns {
		if colName == "time" || colName == "timestamp" {
			continue
		}
		colLen := d.getSliceLength(colData)
		if colLen > 0 {
			dataType := d.getSliceType(colData)
			fields = append(fields, arrow.Field{
				Name: colName,
				Type: dataType,
			})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}
