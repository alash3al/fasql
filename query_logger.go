package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// QueryLogger represents a logger for all write queries (just like the binary logs in mysql)
type QueryLogger struct {
	mem  *sqlx.DB
	disk *sqlx.DB
}

// NewQueryLogger initializes a new query logger
func NewQueryLogger() (*QueryLogger, error) {
	memdb, err := sqlx.Connect("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		return nil, err
	}
	memdb.SetConnMaxLifetime(0)

	diskdb, err := sqlx.Connect("sqlite3", "file:./fasql.logs")
	if err != nil {
		return nil, err
	}

	for _, db := range []*sqlx.DB{memdb, diskdb} {
		if _, err := db.Exec("CREATE TABLE IF NOT EXISTS logs(id INTEGER PRIMARY KEY, request, time)"); err != nil {
			return nil, err
		}
	}

	return &QueryLogger{
		mem:  memdb,
		disk: diskdb,
	}, nil
}

// Put writes a new record into the store
func (ql *QueryLogger) Put(req QueryRequest) error {
	j, err := json.Marshal(req)

	if err != nil {
		return err
	}

	if _, err := ql.mem.Exec("INSERT INTO logs(request, time) VALUES(?, ?)", string(j), time.Now().UnixNano()); err != nil {
		return err
	}

	return nil
}

// Sync poll the specified n of records from the memory and write to the disk
func (ql *QueryLogger) Sync(n int) error {
	var records []QueryLoggerRecord

	if err := ql.mem.Select(&records, fmt.Sprintf("SELECT * FROM logs LIMIT %d", n)); err != nil {
		return err
	}

	valuesPlaceholders := []string{}
	valuesArgs := []interface{}{}

	for _, record := range records {
		valuesPlaceholders = append(valuesPlaceholders, "(?, ?)")
		valuesArgs = append(valuesArgs, record.Request, record.Time)
	}

	insertQuery := "INSERT INTO logs(request, time) VALUES" + strings.Join(valuesPlaceholders, ", ")

	if _, err := ql.disk.Exec(insertQuery, valuesArgs...); err != nil {
		return err
	}

	if _, err := ql.mem.Exec("DELETE FROM logs WHERE id <= ?", records[len(records)-1].ID); err != nil {
		return err
	}

	return nil
}
