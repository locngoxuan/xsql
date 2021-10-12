package xsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

// DeleteById deletes specific entity by id
func DeleteById(model interface{}) (int64, error) {
	return DeleteByIdContext(context.Background(), model)
}

// DeleteByIdTx deletes specific entity by id within transaction
func DeleteByIdTx(tx *sql.Tx, model interface{}) (int64, error) {
	return DeleteByIdTxContext(context.Background(), tx, model)
}

// DeleteByIdTx deletes specific entity by id in a specific context
func DeleteByIdContext(ctx context.Context, model interface{}) (int64, error) {
	return execTransaction(ctx, func(tx *sql.Tx) (int64, error) {
		return DeleteByIdTxContext(ctx, tx, model)
	})
}

// DeleteByIdTx deletes specific entity by id within transaction and a specific context
func DeleteByIdTxContext(ctx context.Context, tx *sql.Tx, model interface{}) (int64, error) {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	tableName := getTableName(val)

	var id interface{}
	for _, k := range [...]string{
		"id", "Id", "ID",
	} {
		field := val.FieldByName(k)
		if !field.IsValid() {
			continue
		}
		id = field.Interface()
		break
	}
	if id == nil {
		return 0, fmt.Errorf(`interface does not have id field`)
	}
	val.FieldByName("id").Interface()

	return DeleteTxContext(ctx, tx, NewStmt(`DELETE FROM `).AppendSql(tableName).
		AppendSql(`WHERE id = :id`).
		With(map[string]interface{}{
			"id": id,
		}).
		Get())
}

// Delete execute a sepecified delete statement
func Delete(statement Statement) (int64, error) {
	return DeleteContext(context.Background(), statement)
}

// Delete execute a sepecified delete statement
func DeleteContext(ctx context.Context, statement Statement) (int64, error) {
	return execTransaction(ctx, func(tx *sql.Tx) (int64, error) {
		return DeleteTxContext(ctx, tx, statement)
	})
}

// Delete execute a sepecified delete statement within a transaction
func DeleteTx(tx *sql.Tx, statement Statement) (int64, error) {
	return DeleteTxContext(context.Background(), tx, statement)
}

// Delete execute a sepecified delete statement within a transaction and a specific context
func DeleteTxContext(ctx context.Context, tx *sql.Tx, statement Statement) (int64, error) {
	defer func(start time.Time) {
		if statement.skipLog {
			return
		}
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
			return 0, ErrWrongNumberAffectedRow
		}
	}
	return i, nil
}
