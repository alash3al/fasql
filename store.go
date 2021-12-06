package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	logsDBName = "__fasql_logs__"
)

// Store represents the main storage engine
type Store struct {
	databases      map[string]*sqlx.DB
	databasesMutex *sync.RWMutex
	logsMemDB      *sqlx.DB
	logsDiskDB     *sqlx.DB
}

// NewStore initialize a new store
func NewStore() (*Store, error) {
	store := &Store{
		databases:      make(map[string]*sqlx.DB),
		databasesMutex: &sync.RWMutex{},
	}

	memdb, err := store.getOrCreateDatabase(logsDBName)
	if err != nil {
		return nil, err
	}

	store.logsMemDB = memdb

	if _, err := store.logsMemDB.Exec("CREATE TABLE IF NOT EXISTS logs(req)"); err != nil {
		return nil, err
	}

	store.logsDiskDB, err = sqlx.Connect("sqlite3", "file:./logs.fasql.sqlite3?_journal=wal")
	if err != nil {
		return nil, err
	}

	if _, err := store.logsDiskDB.Exec("CREATE TABLE IF NOT EXISTS logs(req)"); err != nil {
		return nil, err
	}

	go (func() {
		for {
			var out []struct {
				Req string `json:"req"`
			}
			if err := store.logsMemDB.Select(&out, "SELECT req FROM logs LIMIT 1000"); err != nil {
				log.Println("[LogsMemDB::Read] =>", err.Error())
				continue
			}
			for _, req := range out {
				if _, err := store.logsDiskDB.Exec("INSERT INTO logs(req) VALUES(?)", req.Req); err != nil {
					log.Println("[LogsDiskDB::Write] =>", err.Error())
					continue
				}
				if _, err := store.logsMemDB.Exec("DELETE FROM logs LIMIT 1"); err != nil {
					log.Println("[LogsMemDB::Delete] =>", err.Error())
				}
			}

			time.Sleep(time.Second * 5)
		}
	})()

	return store, nil
}

// ListDatabases list all available databases
func (s *Store) ListDatabases() []string {
	s.databasesMutex.RLock()
	defer s.databasesMutex.RUnlock()

	databases := make([]string, len(s.databases))
	for dbname := range s.databases {
		databases = append(databases, dbname)
	}

	return databases
}

// Write runs a write query on the specified database
func (s *Store) Write(dbname string, req QueryRequest) (*WriteQueryResult, error) {
	db, err := s.getOrCreateDatabase(dbname)
	if err != nil {
		return nil, err
	}

	result, err := db.Exec(req.Query, req.Args...)
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

	s.appendToLog(req)

	return &WriteQueryResult{
		LastInsertedID: lastInsertedID,
		AffectedRows:   affectedRows,
	}, nil
}

// Read runs a read query on the specified database
func (s *Store) Read(dbname string, req QueryRequest) ([]map[string]interface{}, error) {
	db, err := s.getOrCreateDatabase(dbname)
	if err != nil {
		return nil, err
	}

	rows, err := db.Queryx(req.Query, req.Args...)
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

func (s *Store) getOrCreateDatabase(dbname string) (*sqlx.DB, error) {
	s.databasesMutex.RLock()
	db, found := s.databases[dbname]
	s.databasesMutex.RUnlock()

	if found {
		return db, nil
	}

	s.databasesMutex.Lock()
	defer s.databasesMutex.Unlock()

	db, err := sqlx.Connect("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(0)

	s.databases[dbname] = db

	return db, nil
}

func (s *Store) appendToLog(req QueryRequest) {
	j, _ := json.Marshal(req)

	if _, err := s.logsMemDB.Exec("INSERT INTO logs(req) VALUES(?)", string(j)); err != nil {
		log.Println("[AppendToLog] => ", err.Error())
	}
}
