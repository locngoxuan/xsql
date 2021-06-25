package xsql

import "fmt"

type SQLiteDialect struct {
}

func (SQLiteDialect) Parameterizie(numberOfValue int) []string {
	var rs []string
	for i := 0; i < numberOfValue; i++ {
		rs = append(rs, "?")
	}
	return rs
}

type MySQLDialect struct {
	SQLiteDialect
}

type PostgreDialect struct {
}

func (PostgreDialect) Parameterizie(numberOfValue int) []string {
	var rs []string
	for i := 0; i < numberOfValue; i++ {
		rs = append(rs, fmt.Sprintf(`$%d`, i+1))
	}
	return rs
}

type OracleDialect struct {
}

func (OracleDialect) Parameterizie(numberOfValue int) []string {
	var rs []string
	for i := 0; i < numberOfValue; i++ {
		rs = append(rs, fmt.Sprintf(`:%d`, i+1))
	}
	return rs
}
