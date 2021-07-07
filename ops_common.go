package xsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

func transact(db *sql.DB, txFunc func(*sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	err = txFunc(tx)
	return err
}

// ExecuteTxContext executes any statement within a transaction and a specific context
func ExecuteTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	return execTxContext(ctx, tx, statement.String(), statement.GetParams()...)
}

// Executes executes a batch of statement
func Executes(statement Statement, args ...map[string]interface{}) (int64, error) {
	return ExecutesContext(context.Background(), statement)
}

// ExecutesContext executes a batch of statement within a specific context
func ExecutesContext(ctx context.Context, statement Statement, args ...map[string]interface{}) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	i, err := ExecutesTxContext(ctx, tx, statement)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return i, nil
}

// ExecutesTx executes a batch of statement within a transaction
func ExecutesTx(tx *sql.Tx, statement Statement, args ...map[string]interface{}) (int64, error) {
	return ExecutesTxContext(context.Background(), tx, statement)
}

// ExecutesTxContext executes a batch of statement within a transaction and a specific context
func ExecutesTxContext(ctx context.Context, tx *sql.Tx, statement Statement, args ...map[string]interface{}) (int64, error) {
	defer func(start time.Time) {
		if statement.skipLog {
			return
		}
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - execute a batch of statement", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(),
			"stmt", statement.RawSql(), "total_item", len(args))
	}(time.Now())
	rowsAffected := int64(0)
	for _, arg := range args {
		stmt := NewStmt(statement.RawSql()).With(arg)
		i, err := ExecuteTxContext(ctx, tx, stmt.Get())
		if err != nil {
			return 0, err
		}
		rowsAffected += i
	}
	if statement.expectedRows > 0 {
		if rowsAffected != statement.expectedRows {
			return 0, ErrWrongNumberAffectedRow
		}
	}
	return int64(rowsAffected), nil
}

// Count returns the total items in corresponding table of given interface
func Count(model interface{}) (int64, error) {
	return CountContext(context.Background(), model)
}

// Count returns the total items in corresponding table of given interface
func CountContext(ctx context.Context, model interface{}) (int64, error) {
	if model == nil {
		return 0, fmt.Errorf("given model is nil")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	count, err := CountTxContext(ctx, tx, model)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	_ = tx.Commit()
	return int64(count), nil
}

func CountTx(tx *sql.Tx, model interface{}) (int64, error) {
	return CountTxContext(context.Background(), tx, model)
}

func CountTxContext(ctx context.Context, tx *sql.Tx, model interface{}) (int64, error) {
	if model == nil {
		return 0, fmt.Errorf("given model is nil")
	}
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	tableName := getTableName(val)

	sql := fmt.Sprintf(`SELECT count(id) FROM %s WHERE 1=1`, tableName)
	defer func(start time.Time) {
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - count total items in table", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(), "stmt", sql)
	}(time.Now())
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	row := stmt.QueryRowContext(ctx)
	if row.Err() != nil {
		return 0, row.Err()
	}
	count := 0
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

// Count returns the number of item fit with given statement
func CountWithCond(statement Statement) (int64, error) {
	return CountWithCondContext(context.Background(), statement)
}

// CountWithCondContext returns the number of item fit with given statement
func CountWithCondContext(ctx context.Context, statement Statement) (int64, error) {
	sql := statement.String()
	defer func(start time.Time) {
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - count with condition", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(), "stmt", sql, "params", statement.params)
	}(time.Now())
	stmt, err := db.PrepareContext(ctx, sql)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	row := stmt.QueryRowContext(ctx, statement.GetParams()...)
	if row.Err() != nil {
		return 0, row.Err()
	}
	count := 0
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return int64(count), nil
}

// execTxContext is private function which executes given sql statement
// and returns number of affected row
func execTxContext(ctx context.Context, tx *sql.Tx, sql string, params ...interface{}) (int64, error) {
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	rs, err := stmt.ExecContext(ctx, params...)
	if err != nil {
		return 0, err
	}
	i, err := rs.RowsAffected()
	if err != nil {
		return 0, err
	}
	return i, nil
}

// execTxContext is private function which returns result as *row
// by executing sql statement with given params
func queryTxContext(ctx context.Context, tx *sql.Tx, sql string, params ...interface{}) (*sql.Stmt, *sql.Rows, error) {
	stmt, err := tx.PrepareContext(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	rows, err := stmt.QueryContext(ctx, params...)
	if err != nil {
		return nil, nil, err
	}
	return stmt, rows, nil
}
