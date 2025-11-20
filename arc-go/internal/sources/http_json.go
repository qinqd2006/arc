package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/arc-core/arc-go/internal/database"
	"github.com/sirupsen/logrus"
)

// HTTPJSONClient fetches data from HTTP JSON endpoints
type HTTPJSONClient struct {
	connection *database.HTTPJSONConnection
	client     *http.Client
	logger     *logrus.Logger
}

// NewHTTPJSONClient creates a new HTTP JSON client
func NewHTTPJSONClient(conn *database.HTTPJSONConnection, logger *logrus.Logger) *HTTPJSONClient {
	if logger == nil {
		logger = logrus.New()
	}

	return &HTTPJSONClient{
		connection: conn,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger,
	}
}

// FetchData fetches data from the HTTP JSON endpoint
func (c *HTTPJSONClient) FetchData(ctx context.Context, params map[string]string) ([]map[string]interface{}, error) {
	// Build request
	req, err := c.buildRequest(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	// Execute request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	// Extract data array
	records, err := c.extractRecords(data)
	if err != nil {
		return nil, err
	}

	// Apply field mappings if configured
	if c.connection.FieldMappings != nil {
		records = c.applyFieldMappings(records)
	}

	c.logger.WithFields(logrus.Fields{
		"endpoint": c.connection.EndpointURL,
		"records":  len(records),
	}).Debug("Fetched data from HTTP JSON endpoint")

	return records, nil
}

// buildRequest constructs the HTTP request
func (c *HTTPJSONClient) buildRequest(ctx context.Context, params map[string]string) (*http.Request, error) {
	method := c.connection.HTTPMethod
	if method == "" {
		method = "GET"
	}

	req, err := http.NewRequestWithContext(ctx, method, c.connection.EndpointURL, nil)
	if err != nil {
		return nil, err
	}

	// Add query parameters
	if c.connection.QueryParams != nil {
		// Parse stored query params
		var queryParams map[string]string
		if err := json.Unmarshal([]byte(*c.connection.QueryParams), &queryParams); err == nil {
			q := req.URL.Query()
			for k, v := range queryParams {
				q.Add(k, v)
			}
			// Add dynamic params
			for k, v := range params {
				q.Set(k, v)
			}
			req.URL.RawQuery = q.Encode()
		}
	}

	// Add headers
	req.Header.Set("Accept", "application/json")
	if c.connection.Headers != nil {
		var headers map[string]string
		if err := json.Unmarshal([]byte(*c.connection.Headers), &headers); err == nil {
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}
	}

	// Add authentication
	if c.connection.AuthType != nil && c.connection.AuthConfig != nil {
		c.addAuthentication(req)
	}

	return req, nil
}

// addAuthentication adds authentication to the request
func (c *HTTPJSONClient) addAuthentication(req *http.Request) {
	if c.connection.AuthType == nil || c.connection.AuthConfig == nil {
		return
	}

	var authConfig map[string]string
	if err := json.Unmarshal([]byte(*c.connection.AuthConfig), &authConfig); err != nil {
		c.logger.WithError(err).Warn("Failed to parse auth config")
		return
	}

	switch *c.connection.AuthType {
	case "bearer":
		if token, ok := authConfig["token"]; ok {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	case "basic":
		if username, ok := authConfig["username"]; ok {
			if password, ok := authConfig["password"]; ok {
				req.SetBasicAuth(username, password)
			}
		}
	case "api_key":
		if key, ok := authConfig["api_key"]; ok {
			if header, ok := authConfig["header"]; ok {
				req.Header.Set(header, key)
			} else {
				req.Header.Set("X-API-Key", key)
			}
		}
	}
}

// extractRecords extracts array of records from response
func (c *HTTPJSONClient) extractRecords(data interface{}) ([]map[string]interface{}, error) {
	// If data_path is configured, navigate to it
	if c.connection.DataPath != nil && *c.connection.DataPath != "" {
		data = navigateJSONPath(data, *c.connection.DataPath)
	}

	// Convert to array of maps
	switch v := data.(type) {
	case []interface{}:
		records := make([]map[string]interface{}, 0, len(v))
		for _, item := range v {
			if record, ok := item.(map[string]interface{}); ok {
				records = append(records, record)
			}
		}
		return records, nil
	case map[string]interface{}:
		// Single object, wrap in array
		return []map[string]interface{}{v}, nil
	default:
		return nil, fmt.Errorf("unexpected data type: %T", data)
	}
}

// applyFieldMappings applies field name mappings
func (c *HTTPJSONClient) applyFieldMappings(records []map[string]interface{}) []map[string]interface{} {
	var mappings map[string]string
	if err := json.Unmarshal([]byte(*c.connection.FieldMappings), &mappings); err != nil {
		c.logger.WithError(err).Warn("Failed to parse field mappings")
		return records
	}

	mapped := make([]map[string]interface{}, len(records))
	for i, record := range records {
		mappedRecord := make(map[string]interface{})
		for sourceField, targetField := range mappings {
			if value, ok := record[sourceField]; ok {
				mappedRecord[targetField] = value
			}
		}
		// Copy unmapped fields
		for k, v := range record {
			if _, mapped := mappings[k]; !mapped {
				mappedRecord[k] = v
			}
		}
		mapped[i] = mappedRecord
	}

	return mapped
}

// TestConnection tests the HTTP JSON connection
func (c *HTTPJSONClient) TestConnection(ctx context.Context) error {
	_, err := c.FetchData(ctx, map[string]string{"limit": "1"})
	return err
}

// Helper function to navigate JSON path (simplified)
func navigateJSONPath(data interface{}, path string) interface{} {
	// Simple implementation - just return data for now
	// In a real implementation, this would parse dot notation like "data.items"
	if m, ok := data.(map[string]interface{}); ok {
		if v, exists := m[path]; exists {
			return v
		}
	}
	return data
}
