package xsql

import (
	"database/sql"
	"fmt"
)

var (
	dialect Dialect
	db      *sql.DB
	logger  Logger
)

func BeginTx() (*sql.Tx, error) {
	return db.Begin()
}

func Open(opt DbOption) error {
	var err error
	if opt.DB != nil {
		//it is configured from outside
		db = opt.DB
	} else {
		db, err = sql.Open(opt.Driver, opt.DSN)
		if err != nil {
			return err
		}
		db.SetMaxOpenConns(opt.MaxOpenConns)
		db.SetMaxIdleConns(opt.MaxIdleConns)
		db.SetConnMaxIdleTime(opt.MaxIdleTime)
		db.SetConnMaxLifetime(opt.MaxLifeTime)
	}

	if opt.Dialect == nil {
		return fmt.Errorf(`db dialect is not configured`)
	}
	dialect = opt.Dialect

	if opt.Logger == nil {
		logger = DefaultLogger{}
	} else {
		logger = opt.Logger
	}
	return nil
}

func Close() error {
	if db == nil {
		return fmt.Errorf(`db is not initiated`)
	}
	return db.Close()
}
