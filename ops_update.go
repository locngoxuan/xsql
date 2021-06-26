package xsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// Update execute a sepecified update statement
func Update(statement Statement) (int64, error) {
	return UpdateContext(context.Background(), statement)
}

// Update execute a sepecified update statement
func UpdateContext(ctx context.Context, statement Statement) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	i, err := UpdateTxContext(ctx, tx, statement)
	if err != nil {
		return 0, err
	}
	if statement.expectedRows > 0 {
		if i != statement.expectedRows {
			_ = tx.Rollback()
			return 0, ErrWrongNumberAffectedRow
		}
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return i, nil
}

// Update execute a sepecified update statement within a transaction
func UpdateTx(tx *sql.Tx, statement Statement) (int64, error) {
	return UpdateTxContext(context.Background(), tx, statement)
}

// Update execute a sepecified update statement within a transaction and an specific context
func UpdateTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	logger.Infow("xsql - execute update statement", "id", ctx.Value("id"),
		"stmt", statement.String(), "params", statement.params)
	return ExecuteTxContext(ctx, tx, statement)
}

func Updates(statement Statement) (int64, error) {
	return UpdatesContext(context.Background(), statement)
}

func UpdatesContext(ctx context.Context, statement Statement) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	i, err := UpdatesTxContext(ctx, tx, statement)
	if err != nil {
		return 0, err
	}
	if statement.expectedRows > 0 {
		if i != statement.expectedRows {
			_ = tx.Rollback()
			return 0, ErrWrongNumberAffectedRow
		}
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return i, nil
}

func UpdatesTx(tx *sql.Tx, statement Statement) (int64, error) {
	return UpdateTxContext(context.Background(), tx, statement)
}

func UpdatesTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	val := reflect.ValueOf(statement.params)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		return 0, fmt.Errorf(`argument must be either array or slice`)
	}
	totalItem := val.Len()
	rowsAffected := int64(0)
	for i := 0; i < totalItem; i++ {
		newStmt := NewStmt(statement.RawSql()).With(val.Index(i).Interface().([]interface{})...)
		i, err := execTxContext(ctx, tx, newStmt.String(), newStmt.params...)
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		rowsAffected = rowsAffected + i
	}
	return rowsAffected, nil
}
