package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Client is the Arc API client
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
	database   string
}

// Config holds client configuration
type Config struct {
	BaseURL    string        // API base URL (e.g., "http://localhost:8000")
	Token      string        // API authentication token
	Database   string        // Default database name (default: "default")
	Timeout    time.Duration // HTTP timeout (default: 30s)
	MaxRetries int           // Maximum retry attempts (default: 3)
}

// NewClient creates a new Arc API client
func NewClient(config Config) *Client {
	if config.BaseURL == "" {
		config.BaseURL = "http://localhost:8000"
	}
	if config.Database == "" {
		config.Database = "default"
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	return &Client{
		baseURL:  config.BaseURL,
		token:    config.Token,
		database: config.Database,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// ColumnarData represents data in columnar format for MessagePack writes
type ColumnarData struct {
	Measurement string                 `msgpack:"m" json:"m"`
	Columns     map[string]interface{} `msgpack:"columns" json:"columns"`
}

// QueryRequest represents a SQL query request
type QueryRequest struct {
	SQL      string                 `json:"sql"`
	Database string                 `json:"database,omitempty"`
	Format   string                 `json:"format,omitempty"`
	Options  map[string]interface{} `json:"options,omitempty"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	Columns  []ColumnInfo    `json:"column_info"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int64           `json:"row_count"`
	Status   string          `json:"status"`
	Message  string          `json:"message,omitempty"`
}

// ColumnInfo represents column metadata
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// WriteResponse represents a write response
type WriteResponse struct {
	Status       string `json:"status"`
	Database     string `json:"database"`
	Measurements int    `json:"measurements"`
	Points       int    `json:"points"`
	Rows         int    `json:"rows"`
	Took         string `json:"took"`
}

// WriteMsgpack writes data in columnar MessagePack format (recommended - fastest)
// Example:
//
//	data := &client.ColumnarData{
//	    Measurement: "cpu",
//	    Columns: map[string]interface{}{
//	        "time":       []int64{time.Now().UnixMilli(), time.Now().UnixMilli() + 1000},
//	        "host":       []string{"server01", "server02"},
//	        "usage_idle": []float64{95.0, 85.0},
//	    },
//	}
//	err := client.WriteMsgpack(ctx, data)
func (c *Client) WriteMsgpack(ctx context.Context, data *ColumnarData) error {
	return c.WriteMsgpackToDatabase(ctx, data, c.database)
}

// WriteMsgpackToDatabase writes MessagePack data to a specific database
func (c *Client) WriteMsgpackToDatabase(ctx context.Context, data *ColumnarData, database string) error {
	// Encode data to MessagePack
	body, err := msgpack.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal msgpack: %w", err)
	}

	// Create request
	url := fmt.Sprintf("%s/api/v1/write/msgpack", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))
	req.Header.Set("x-arc-database", database)

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check response
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// WriteLineProtocol writes data in InfluxDB line protocol format
// Example:
//
//	lineProtocol := "cpu,host=server01,region=us-east usage_idle=95.0,usage_user=3.2 1234567890000000000"
//	err := client.WriteLineProtocol(ctx, lineProtocol)
func (c *Client) WriteLineProtocol(ctx context.Context, lineProtocol string) (*WriteResponse, error) {
	return c.WriteLineProtocolToDatabase(ctx, lineProtocol, c.database)
}

// WriteLineProtocolToDatabase writes line protocol data to a specific database
func (c *Client) WriteLineProtocolToDatabase(ctx context.Context, lineProtocol string, database string) (*WriteResponse, error) {
	url := fmt.Sprintf("%s/api/v1/write?db=%s", c.baseURL, database)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(lineProtocol))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("write failed with status %d: %s", resp.StatusCode, string(body))
	}

	var writeResp WriteResponse
	if err := json.Unmarshal(body, &writeResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &writeResp, nil
}

// Query executes a SQL query and returns results in JSON format
// Example:
//
//	response, err := client.Query(ctx, "SELECT * FROM cpu LIMIT 10")
func (c *Client) Query(ctx context.Context, sql string) (*QueryResponse, error) {
	return c.QueryWithOptions(ctx, sql, c.database, nil)
}

// QueryWithOptions executes a SQL query with custom options
func (c *Client) QueryWithOptions(ctx context.Context, sql, database string, options map[string]interface{}) (*QueryResponse, error) {
	reqBody := QueryRequest{
		SQL:      sql,
		Database: database,
		Format:   "json",
		Options:  options,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/query", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var queryResp QueryResponse
	if err := json.Unmarshal(respBody, &queryResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &queryResp, nil
}

// QueryArrow executes a SQL query and returns results in Apache Arrow format
func (c *Client) QueryArrow(ctx context.Context, sql string) ([]byte, error) {
	return c.QueryArrowWithDatabase(ctx, sql, c.database)
}

// QueryArrowWithDatabase executes a SQL query and returns results in Apache Arrow format from a specific database
func (c *Client) QueryArrowWithDatabase(ctx context.Context, sql, database string) ([]byte, error) {
	reqBody := QueryRequest{
		SQL:      sql,
		Database: database,
		Format:   "arrow",
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/query/arrow", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// QueryParquet executes a SQL query and saves results to a Parquet file
func (c *Client) QueryParquet(ctx context.Context, sql, filename string) error {
	return c.QueryParquetWithDatabase(ctx, sql, filename, c.database)
}

// QueryParquetWithDatabase executes a SQL query and saves results to a Parquet file from a specific database
func (c *Client) QueryParquetWithDatabase(ctx context.Context, sql, filename, database string) error {
	reqBody := QueryRequest{
		SQL:      sql,
		Database: database,
		Format:   "parquet",
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/query", c.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Create the output file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close()

	// Copy response body to file
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to write parquet data to file: %w", err)
	}

	return nil
}

// StreamCallback is called for each row in a streaming query
type StreamCallback func(row []string) error

// QueryStream executes a streaming SQL query and calls the callback for each row
// Example:
//
//	err := client.QueryStream(ctx, "SELECT * FROM cpu", func(row []string) error {
//	    fmt.Println("Row:", row)
//	    return nil
//	})
func (c *Client) QueryStream(ctx context.Context, sql string, callback StreamCallback) error {
	return c.QueryStreamWithDatabase(ctx, sql, c.database, callback)
}

// QueryStreamWithDatabase executes a streaming SQL query from a specific database
func (c *Client) QueryStreamWithDatabase(ctx context.Context, sql, database string, callback StreamCallback) error {
	url := fmt.Sprintf("%s/api/v1/query/stream", c.baseURL)

	// Create form data
	formData := fmt.Sprintf("sql=%s&db=%s", sql, database)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(formData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.token))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("stream query failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Read CSV stream
	reader := io.Reader(resp.Body)
	buf := make([]byte, 4096)
	var line []byte

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			line = append(line, buf[:n]...)

			// Process complete lines
			for {
				idx := bytes.IndexByte(line, '\n')
				if idx == -1 {
					break
				}

				rowData := string(line[:idx])
				line = line[idx+1:]

				// Parse CSV row (simplified)
				row := parseCSVRow(rowData)
				if err := callback(row); err != nil {
					return err
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read stream: %w", err)
		}
	}

	return nil
}

// parseCSVRow parses a simple CSV row
func parseCSVRow(row string) []string {
	// Simple CSV parser - for production use encoding/csv
	var result []string
	var current string
	inQuotes := false

	for _, char := range row {
		switch char {
		case '"':
			inQuotes = !inQuotes
		case ',':
			if !inQuotes {
				result = append(result, current)
				current = ""
			} else {
				current += string(char)
			}
		default:
			current += string(char)
		}
	}
	result = append(result, current)

	return result
}

// SetDatabase sets the default database for subsequent operations
func (c *Client) SetDatabase(database string) {
	c.database = database
}

// GetDatabase returns the current default database
func (c *Client) GetDatabase() string {
	return c.database
}
