package xsql

import (
	"context"
	"database/sql"
)

// Update execute a sepecified update statement
func Update(statement Statement) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	i, err := UpdateTx(tx, statement)
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	return i, err
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
