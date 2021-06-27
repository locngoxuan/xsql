package xsql

import (
	"context"
	"database/sql"
	"time"
)

// Delete execute a sepecified delete statement
func Delete(statement Statement) (int64, error) {
	return DeleteContext(context.Background(), statement)
}

// Delete execute a sepecified delete statement
func DeleteContext(ctx context.Context, statement Statement) (int64, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	i, err := DeleteTxContext(ctx, tx, statement)
	if err != nil {
		return 0, err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return i, nil
}

// Delete execute a sepecified delete statement within a transaction
func DeleteTx(tx *sql.Tx, statement Statement) (int64, error) {
	return DeleteTxContext(context.Background(), tx, statement)
}

// Delete execute a sepecified delete statement within a transaction and an specific context
func DeleteTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	defer func(start time.Time) {
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - execute delete statement", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(),
			"stmt", statement.String(), "params", statement.params)
	}(time.Now())
	i, err := ExecuteTxContext(ctx, tx, statement)
	if err != nil {
		return 0, err
	}
	if statement.expectedRows > 0 {
		if i != statement.expectedRows {
			_ = tx.Rollback()
			return 0, ErrWrongNumberAffectedRow
		}
	}
	return i, nil
}
