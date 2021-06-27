package xsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Insert adds given interface into corresponding table
func Insert(model interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = InsertTx(tx, model)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

// Insert adds given interface into corresponding table
func InsertContext(ctx context.Context, model interface{}) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	err = InsertTxContext(ctx, tx, model)
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return err
}

// Insert adds given interface into corresponding table within a transaction
func InsertTx(tx *sql.Tx, model interface{}) error {
	return InsertTxContext(context.Background(), tx, model)
}

// Insert adds given interface into corresponding table within a transaction and context
func InsertTxContext(ctx context.Context, tx *sql.Tx, model interface{}) error {
	start := time.Now()
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
		return ErrArgIsArrayOrSlice
	}
	tableName := getTableName(val)
	columns, fieldNames := getColumnsAndFielNames(val.Type())
	if len(columns) != len(fieldNames) {
		return fmt.Errorf(`size of column and size of field does not match`)
	}
	args := make([]interface{}, len(columns))
	for i, fieldName := range fieldNames {
		args[i] = val.FieldByName(fieldName).Interface()
	}

	sqlScript := fmt.Sprintf(`INSERT INTO %s(%s) VALUES (:value)`,
		tableName,
		strings.Join(columns, ","),
	)

	defer func(start time.Time) {
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - execute insert statement", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(),
			"stmt", sqlScript, "params", args)
	}(start)

	insertCmd := NewStmt(sqlScript).With(map[string]interface{}{
		"value": args,
	})
	i, err := ExecuteTxContext(ctx, tx, *insertCmd)
	if err != nil {
		return err
	}
	if i == 0 {
		return ErrWrongNumberInserted
	}
	return nil
}

// InsertBatch creates a batch of item in corresponding table of that interface.
func InsertBatch(model interface{}, batchSize int) error {
	return InsertBatchContext(context.Background(), model, batchSize)
}

// InsertBatch creates a batch of item in corresponding table of that interface.
func InsertBatchContext(ctx context.Context, model interface{}, batchSize int) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	err = InsertBatchTxContext(ctx, tx, model, batchSize)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

// InsertBatch creates a batch of item in corresponding table of that interface within a transaction
func InsertBatchTx(tx *sql.Tx, model interface{}, batchSize int) error {
	return InsertBatchTxContext(context.Background(), tx, model, batchSize)
}

// InsertBatch creates a batch of item in corresponding table of that interface within a transaction
// and a specific context
func InsertBatchTxContext(ctx context.Context, tx *sql.Tx, model interface{}, batchSize int) error {
	start := time.Now()
	val := reflect.ValueOf(model)
	if val.Kind() != reflect.Array && val.Kind() != reflect.Slice {
		_ = tx.Rollback()
		return ErrArgNotArrayAndSlice
	}

	if val.Len() == 0 {
		return nil
	}

	fe := val.Index(0)
	tableName := getTableName(fe)
	columns, fieldNames := getColumnsAndFielNames(fe.Type())
	if len(columns) != len(fieldNames) {
		return fmt.Errorf(`size of column and size of field does not match`)
	}

	insertedBatches := chunk(val, batchSize)
	sqlColumns := strings.Join(columns, ",")

	defer func(start time.Time) {
		elapsed := time.Now().Sub(start)
		logger.Infow("xsql - execute insert-batch statement", "id", ctx.Value("id"),
			"elapsed_time", elapsed.Milliseconds(),
			"stmt", fmt.Sprintf(`INSERT INTO %s(%s) VALUES (:value)`, tableName, sqlColumns),
			"total_item", val.Len(), "batch_size", batchSize)
	}(start)

	numberOfField := len(fieldNames)
	paramPlaceHolder := strRepeat("(", ")", "%s", ",", len(columns))
	sqlParams := strRepeat("", "", paramPlaceHolder, ",", batchSize)
	for _, batch := range insertedBatches {

		values := make([]interface{}, len(batch)*numberOfField)
		for i, v := range batch {
			for j, fieldName := range fieldNames {
				values[i*numberOfField+j] = v.FieldByName(fieldName).Interface()
			}
		}

		if batchSize != len(batch) {
			sqlParams = strRepeat("", "", paramPlaceHolder, ",", batchSize)
		}
		realInsertSql := fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s`,
			tableName,
			sqlColumns,
			fmt.Sprintf(sqlParams, strToIntf(dialect.Parameterizie(len(values)))...),
		)

		i, err := execTxContext(ctx, tx, realInsertSql, values...)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		if int(i) != len(batch) {
			_ = tx.Rollback()
			return ErrWrongNumberInserted
		}
	}
	return nil
}
