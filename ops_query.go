package xsql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// Query return a slice of records
func Query(statement Statement, output interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = QueryTx(tx, statement, output)
	if err == nil {
		_ = tx.Commit()
	}
	return err
}

// Query return a slice of records. By giving a specific transaction, it may get a latest records
// which are inserted/updated in previous actions during given transaction
func QueryTx(tx *sql.Tx, statement Statement, output interface{}) error {
	return QueryTxContext(context.Background(), tx, statement, output)
}

// Query return a slice of records. By giving a specific transaction, it may get a latest records
// which are inserted/updated in previous actions during given transaction.
//
// The provided context will be used for the preparation of the context, not
// for the execution of the returned statement. The returned statement
// will run in the transaction context.
func QueryTxContext(ctx context.Context, tx *sql.Tx, statement Statement, output interface{}) error {
	valType := reflect.TypeOf(output)
	if valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
	}

	if valType.Kind() != reflect.Array && valType.Kind() != reflect.Slice {
		_ = tx.Commit()
		return fmt.Errorf("input is not either array or slice")
	}

	valType = valType.Elem()
	rm := getMapper(valType)
	val := reflect.ValueOf(output)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	sql := statement.String()
	logger.Infow("xsql - execute query statement", "id", ctx.Value("id"),
		"stmt", sql, "params", statement.params)
	stmt, rows, err := queryTxContext(ctx, tx, sql, statement.params...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	defer func() {
		_ = rows.Close()
		_ = stmt.Close()
	}()

	cols, err := rows.Columns()
	for rows.Next() {
		ptr := reflect.New(valType)
		elem := ptr.Elem()
		args := make([]interface{}, len(cols))
		for i, v := range cols {
			fieldName, ok := rm.Col2Field[v]
			if !ok {
				_ = tx.Rollback()
				return fmt.Errorf(`no such field mapped to column %s`, v)
			}
			args[i] = elem.FieldByName(fieldName).Addr().Interface()
		}
		e := rows.Scan(args...)
		if e != nil {
			_ = tx.Rollback()
			return e
		}
		val.Set(reflect.Append(val, ptr.Elem()))
	}

	err = rows.Err()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

// QueryOne will returns an item fit given statement if it exist. Otherwise, it return ErrNotFound
func QueryOne(statement Statement, output interface{}) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = QueryOneTx(tx, statement, output)
	if err == nil {
		_ = tx.Commit()
	}
	return err
}

// QueryOne will returns an item fit given statement if it exist. Otherwise, it return ErrNotFound
//
// This action is excuted within a transaction
func QueryOneTx(tx *sql.Tx, statement Statement, output interface{}) error {
	return QueryOneTxContext(context.Background(), tx, statement, output)
}

// QueryOne will returns an item fit given statement if it exist. Otherwise, it return ErrNotFound
//
// This action is excuted within a transaction and a specific context
func QueryOneTxContext(ctx context.Context, tx *sql.Tx, statement Statement, output interface{}) error {
	valType := reflect.TypeOf(output)
	if valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
	}

	if valType.Kind() == reflect.Array || valType.Kind() == reflect.Slice {
		return fmt.Errorf("input is either array or slice")
	}

	rm := getMapper(valType)

	stmt, err := tx.Prepare(statement.String())
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	defer func() {
		_ = stmt.Close()
	}()

	sql := statement.String()
	logger.Infow("xsql - execute query-one statement", "id", ctx.Value("id"),
		"stmt", sql, "params", statement.params)
	stmt, rows, err := queryTxContext(ctx, tx, sql, statement.params...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	defer func() {
		_ = rows.Close()
		_ = stmt.Close()
	}()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	numberOfRows := 0
	for rows.Next() {
		elem := reflect.ValueOf(output).Elem()
		args := make([]interface{}, len(cols))
		for i, v := range cols {
			fieldName, ok := rm.Col2Field[v]
			if !ok {
				_ = tx.Rollback()
				return fmt.Errorf(`no such field mapped to column %s`, v)
			}
			args[i] = elem.FieldByName(fieldName).Addr().Interface()
		}
		e := rows.Scan(args...)
		if e != nil {
			_ = tx.Rollback()
			return e
		}
		numberOfRows++
		break
	}

	err = rows.Err()
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if numberOfRows == 0 {
		return ErrNotFound
	}
	return nil
}
