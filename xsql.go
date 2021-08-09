package xsql

import (
	"context"
	"database/sql"
	"fmt"
)

var (
	dialect Dialect
	db      *sql.DB
	logger  Logger

	isoLevel sql.IsolationLevel = sql.LevelDefault
	readOnly bool               = false
)

func BeginTx() (*sql.Tx, error) {
	return db.Begin()
}

func BeginTxContext(ctx context.Context) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

func Open(opt DbOption) error {
	var err error
	if opt.DB != nil {
		//it is configured from outside
		db = opt.DB
	} else {
		db, err = sql.Open(opt.Driver, opt.DSN)
		if err != nil {
			return fmt.Errorf(`failed to open db connection %v`, err)
		}
		db.SetMaxOpenConns(opt.MaxOpenConns)
		db.SetMaxIdleConns(opt.MaxIdleConns)
		db.SetConnMaxIdleTime(opt.MaxIdleTime)
		db.SetConnMaxLifetime(opt.MaxLifeTime)
	}

	if opt.Dialect == nil {
		dialect, err = getDbDialect(opt.Driver)
		if err != nil {
			return err
		}
	}
	isoLevel = opt.IsoLevel
	readOnly = opt.ReadOnly
	dialect = opt.Dialect
	if dialect == nil {
		return fmt.Errorf(`db dialect is not configured`)
	}
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
