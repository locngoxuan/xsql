package xsql

import "database/sql"

var (
	dialect Dialect
	db      *sql.DB
)

func SetDialect(d Dialect) {
	dialect = d
}

func BeginTx() (*sql.Tx, error) {
	return db.Begin()
}
