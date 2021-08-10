package xsql

import (
	"context"
	"database/sql"
	"time"
)

// Update execute a sepecified update statement
func Update(statement Statement) (int64, error) {
	return UpdateContext(context.Background(), statement)
}

// Update execute a sepecified update statement
func UpdateContext(ctx context.Context, statement Statement) (int64, error) {
	return execTransaction(ctx, func(tx *sql.Tx) (int64, error) {
		return UpdateTxContext(ctx, tx, statement)
	})
}

// Update execute a sepecified update statement within a transaction
func UpdateTx(tx *sql.Tx, statement Statement) (int64, error) {
	return UpdateTxContext(context.Background(), tx, statement)
}

// Update execute a sepecified update statement within a transaction and an specific context
func UpdateTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	defer func(start time.Time) {
		if statement.skipLog {
			return
		}
		elapsed := time.Since(start)
		logger.Infow("xsql - execute update statement", "id", ctx.Value("id"), "elapsed_time", elapsed.Milliseconds(),
			"stmt", statement.String(), "params", statement.params)
	}(time.Now())

	i, err := ExecuteTxContext(ctx, tx, statement)
	if err != nil {
		return 0, err
	}
	if statement.expectedRows > 0 {
		if i != statement.expectedRows {
			return 0, ErrWrongNumberAffectedRow
		}
	}
	return i, nil
}

func Updates(statement Statement, args ...map[string]interface{}) (int64, error) {
	return UpdatesContext(context.Background(), statement)
}

func UpdatesContext(ctx context.Context, statement Statement, args ...map[string]interface{}) (int64, error) {
	return execTransaction(ctx, func(tx *sql.Tx) (int64, error) {
		return UpdatesTxContext(ctx, tx, statement)
	})
}

func UpdatesTx(tx *sql.Tx, statement Statement, args ...map[string]interface{}) (int64, error) {
	return UpdateTxContext(context.Background(), tx, statement)
}

func UpdatesTxContext(ctx context.Context, tx *sql.Tx, statement Statement, args ...map[string]interface{}) (int64, error) {
	defer func(start time.Time) {
		if statement.skipLog {
			return
		}
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - execute update-batch statement", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(),
			"stmt", statement.RawSql(), "total_item", len(args))
	}(time.Now())
	rowsAffected := int64(0)
	for _, arg := range args {
		stmt := NewStmt(statement.RawSql()).With(arg)
		i, err := UpdateTxContext(ctx, tx, stmt.Get())
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
		rowsAffected += i
	}
	if statement.expectedRows > 0 {
		if rowsAffected != statement.expectedRows {
			_ = tx.Rollback()
			return 0, ErrWrongNumberAffectedRow
		}
	}
	return int64(rowsAffected), nil
}
