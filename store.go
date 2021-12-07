package main

import (
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/mattn/go-sqlite3"
)

// Store represents the main storage engine
type Store struct {
	memdb   *sqlx.DB
	memdbFd *sqlite3.SQLiteConn

	diskdb   *sqlx.DB
	diskdbFd *sqlite3.SQLiteConn
}

// NewStore initialize a new store
func NewStore() (*Store, error) {
	registerFasqlExtension()

	memdb, err := sqlx.Connect("fasql", "file::memory:?cache=shared")
	if err != nil {
		return nil, err
	}
	memdb.SetConnMaxLifetime(0)
	memdbFd := getLastRegisteredSqliteConn()

	diskdb, err := sqlx.Connect("fasql", "file:./data/backup.fasql?_journal_mode=wal")
	if err != nil {
		return nil, err
	}
	diskdbFd := getLastRegisteredSqliteConn()

	bk, err := memdbFd.Backup("main", diskdbFd, "main")
	if err != nil {
		return nil, err
	}

	if _, err := bk.Step(-1); err != nil {
		return nil, err
	}

	store := &Store{
		memdb:  memdb,
		diskdb: diskdb,
	}

	return store, nil
}

// Write runs a write query on the specified database
func (s *Store) Write(req QueryRequest) (*WriteQueryResult, error) {
	result, err := s.memdb.Exec(req.Query, req.Args...)
	if err != nil {
		return nil, err
	}

	lastInsertedID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	affectedRows, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if _, err := s.diskdb.Exec(req.Query); err != nil {
		log.Println(err)
	}

	return &WriteQueryResult{
		LastInsertedID: lastInsertedID,
		AffectedRows:   affectedRows,
	}, nil
}

// Read runs a read query on the specified database
func (s *Store) Read(req QueryRequest) ([]map[string]interface{}, error) {
	rows, err := s.memdb.Queryx(req.Query, req.Args...)
	if err != nil {
		return nil, fmt.Errorf("[db.Queryx::Error] => %s", err.Error())
	}

	result := []map[string]interface{}{}

	for rows.Next() {
		record := map[string]interface{}{}

		if err := rows.MapScan(record); err != nil {
			return nil, fmt.Errorf("[rows.MapScan::Error] => %s", err.Error())
		}

		result = append(result, record)
	}

	return result, nil
}
