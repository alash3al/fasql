package main

import (
	"database/sql"
	"sync"

	"github.com/mattn/go-sqlite3"
)

var (
	sqlite3conn      = []*sqlite3.SQLiteConn{}
	sqlite3connMutex = &sync.RWMutex{}
)

func registerFasqlExtension() {
	sql.Register("fasql",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				sqlite3connMutex.Lock()
				sqlite3conn = append(sqlite3conn, conn)
				sqlite3connMutex.Unlock()
				return nil
			},
		})
}

func getLastRegisteredSqliteConn() *sqlite3.SQLiteConn {
	sqlite3connMutex.RLock()
	defer sqlite3connMutex.RUnlock()

	return sqlite3conn[len(sqlite3conn)-1]
}
