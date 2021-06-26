package xsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// Insert adds given interface into corresponding table
func Insert(model interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = InsertTx(tx, model)
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return err
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
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
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

	sqlScript := fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s`,
		tableName,
		strings.Join(columns, ","),
		strRepeat("(", ")", "%s", ",", len(columns)),
	)

	logger.Infow("xsql - execute insert statement", "id", ctx.Value("id"),
		"stmt", sqlScript, "params", args)
	insertCmd := NewStmt(sqlScript).With(args...)
	i, err := ExecuteTxContext(ctx, tx, *insertCmd)
	if err != nil {
		return err
	}
	if i == 0 {
		return fmt.Errorf(`failed to insert new record (row affected = 0)`)
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
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return err
}

// InsertBatch creates a batch of item in corresponding table of that interface within a transaction
func InsertBatchTx(tx *sql.Tx, model interface{}, batchSize int) error {
	return InsertBatchTxContext(context.Background(), tx, model, batchSize)
}

// InsertBatch creates a batch of item in corresponding table of that interface within a transaction
// and a specific context
func InsertBatchTxContext(ctx context.Context, tx *sql.Tx, model interface{}, batchSize int) error {
	val := reflect.ValueOf(model)
	if val.Kind() != reflect.Array && val.Kind() != reflect.Slice {
		_ = tx.Rollback()
		return fmt.Errorf("input is not either array or slice")
	}

	if val.Len() == 0 {
		return nil
	}

	tableName := getTableName(val)
	columns, fieldNames := getColumnsAndFielNames(val.Type())
	if len(columns) != len(fieldNames) {
		return fmt.Errorf(`size of column and size of field does not match`)
	}

	numberOfField := len(fieldNames)
	insertedBatches := chunk(val, batchSize)

	sqlColumns := strings.Join(columns, ",")
	sqlParamOfEachItems := strRepeat("(", ")", "%s", ",", len(columns))

	logger.Infow("xsql - execute insert-batch statement", "id", ctx.Value("id"),
		"stmt", fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s`, tableName, sqlColumns, sqlParamOfEachItems),
		"total_item", val.Len(), "batch_size", batchSize)

	for _, batch := range insertedBatches {
		values := make([]interface{}, len(batch)*numberOfField)
		for i, v := range batch {
			for j, fieldName := range fieldNames {
				values[i*numberOfField+j] = v.FieldByName(fieldName).Interface()
			}
		}
		sqlParams := strRepeat("", "", sqlParamOfEachItems, ",", len(batch))
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
			return fmt.Errorf("can not insert new records in table %s", tableName)
		}
	}
	return nil
}
