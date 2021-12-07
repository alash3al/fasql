package main

// QueryRequest a request to execute the specified query as will its args
type QueryRequest struct {
	Query string        `json:"query"`
	Args  []interface{} `json:"args"`
}

// ErrorResponse represents an http error response
type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

// OkResponse represents a http OK response
type OkResponse struct {
	Success bool `json:"success"`
}

// WriteQueryResult represents the result from a single Write
type WriteQueryResult struct {
	AffectedRows   int64 `json:"affected_rows"`
	LastInsertedID int64 `json:"last_inserted_id"`
}

// WriteQueryResponse represents http write query response
type WriteQueryResponse struct {
	Success bool              `json:"success"`
	Result  *WriteQueryResult `json:"result"`
}

// ReadQueryResponse represents http select query response
type ReadQueryResponse struct {
	Success bool                     `json:"success"`
	Result  []map[string]interface{} `json:"result"`
}

// QueryLoggerRecord represents a record in the query-logger store
type QueryLoggerRecord struct {
	ID      int64  `db:"id"`
	Request string `db:"request"`
	Time    int64  `db:"time"`
}
