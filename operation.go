package xsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

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

func InsertTx(tx *sql.Tx, model interface{}) error {
	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	tableName := getTableName(val)
	columns, fieldNames := getTableInfo(val.Type())
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

	insertCmd := NewStmt(sqlScript).With(args...)
	i, err := ExecuteTx(tx, *insertCmd)
	if err != nil {
		return err
	}
	if i == 0 {
		return fmt.Errorf(`failed to insert new record (row affected = 0)`)
	}
	return nil
}

func InsertBatch(model interface{}, batchSize int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	err = InsertBatchTx(tx, model, batchSize)
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return err
}

func InsertBatchTx(tx *sql.Tx, model interface{}, batchSize int) error {
	val := reflect.ValueOf(model)
	if val.Kind() != reflect.Array && val.Kind() != reflect.Slice {
		_ = tx.Rollback()
		return fmt.Errorf("input is not either array or slice")
	}

	if val.Len() == 0 {
		return nil
	}

	tableName := getTableName(val)
	columns, fieldNames := getTableInfo(val.Type())
	if len(columns) != len(fieldNames) {
		return fmt.Errorf(`size of column and size of field does not match`)
	}

	numberOfField := len(fieldNames)
	chunks := chunk(val, batchSize)
	sqlScript := fmt.Sprintf(`INSERT INTO %s(%s) VALUES %s`,
		tableName,
		strings.Join(columns, ","),
		strRepeat("(", ")", "%s", ",", len(columns)),
	)

	for _, batch := range chunks {
		values := make([]interface{}, len(batch)*numberOfField)
		for i, v := range batch {
			for j, fieldName := range fieldNames {
				values[i*numberOfField+j] = v.FieldByName(fieldName).Interface()
			}
		}
		i, err := execTx(tx, sqlScript, values...)
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

func UpdateTx(tx *sql.Tx, statement Statement) (int64, error) {
	return ExecuteTx(tx, statement)
}

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

func DeleteTx(tx *sql.Tx, statement Statement) (int64, error) {
	return ExecuteTx(tx, statement)
}

func Execute(statement Statement) (int64, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	i, err := ExecuteTx(tx, statement)
	if err == nil {
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			return 0, err
		}
	}
	return i, err
}

func ExecuteTx(tx *sql.Tx, statement Statement) (int64, error) {
	return execTx(tx, statement.String(), statement.params...)
}

func execTx(tx *sql.Tx, sql string, params ...interface{}) (int64, error) {
	stmt, err := tx.Prepare(sql)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	defer func() {
		_ = stmt.Close()
	}()
	rs, err := stmt.Exec(params...)
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	i, err := rs.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return 0, err
	}
	return i, nil
}

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

func queryTx(tx *sql.Tx, sql string, params ...interface{}) (*sql.Rows, error) {
	stmt, err := tx.Prepare(sql)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	defer func() {
		_ = stmt.Close()
	}()

	rows, err := stmt.Query(params...)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return rows, err
}

func QueryTx(tx *sql.Tx, statement Statement, output interface{}) error {
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

	rows, err := queryTx(tx, statement.String(), statement.params...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	defer func() {
		_ = rows.Close()
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

func QueryOneTx(tx *sql.Tx, statement Statement, output interface{}) error {
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

	rows, err := queryTx(tx, statement.String(), statement.params...)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	defer func() {
		_ = rows.Close()
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

func Count(model interface{}) (int64, error) {
	if model == nil {
		return 0, fmt.Errorf("given model is nil")
	}

	val := reflect.ValueOf(model)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	tableName := getTableName(val)
	stmt, err := db.Prepare(fmt.Sprintf(`SELECT count(id) FROM %s WHERE 1=1`, tableName))
	if err != nil {
		return 0, err
	}
	row := stmt.QueryRow()
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

func CountWithCond(statement Statement) (int64, error) {
	stmt, err := db.Prepare(statement.String())
	if err != nil {
		return 0, err
	}
	row := stmt.QueryRow(statement.params...)
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
