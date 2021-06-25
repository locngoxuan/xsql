# XSQL

## Introduction

## Usage
```go
package main

import (
	"fmt"
	"log"

	_ "github.com/lib/pq"
	"github.com/locngoxuan/xsql"
)

type Test struct {
	xsql.BaseModel `column:"__embedded"`
}

func main() {
	err := xsql.Open(xsql.DbOption{
		Driver:       "postgres",
		DSN:          "postgresql://example:example@localhost:5432/example?sslmode=disable",
		MaxOpenConns: 2,
		MaxIdleConns: 1,
		Dialect:      xsql.PostgreDialect{},
	})
	if err != nil {
		log.Fatalln(err)
	}

	var items []Test
	stmt := xsql.NewStmt(`SELECT id, created, updated FROM tests WHERE id = %s`).With(1)
	err = xsql.Query(*stmt, &items)
	if err != nil {
		log.Fatalln(fmt.Sprintf(`failed to execute query %v`, err))
	}
	fmt.Println(items[0])
}
```