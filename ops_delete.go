package xsql

import (
	"context"
	"database/sql"
)

// Delete execute a sepecified delete statement
func Delete(statement Statement) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	i, err := DeleteTx(tx, statement)
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	return i, err
}

// Delete execute a sepecified delete statement within a transaction
func DeleteTx(tx *sql.Tx, statement Statement) (int64, error) {
	return DeleteTxContext(context.Background(), tx, statement)
}

// Delete execute a sepecified delete statement within a transaction and an specific context
func DeleteTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	logger.Infow("xsql - execute delete statement", "id", ctx.Value("id"),
		"stmt", statement.String(), "params", statement.params)
	return ExecuteTxContext(ctx, tx, statement)
}
