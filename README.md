# XSQL

## Introduction

`xsql` is a simple library in order to support us build up a application which:
- Use native sql statement but need to switch database vendors back and forth
- Use application-based id generator

This implementation uses `xsql.Dialect` to provide an interface for replacing `name place holder` by specific parameter place holder of each database vendor. 

Even `xsql` does not intend to be built up as an ORM library, but it also supports: 

- Mapping between column and field of struct
- C in `CRUD` - you can save an item into database by calling `xsql.Insert(interface)` instead of writting insert sql statement

## Usage

```bash
$ go get -d -v github.com/locngoxuan/xsql
```

## Example
```go
package main

import (
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"github.com/locngoxuan/xsql"
)

type ExampleTable struct {
	xsql.BaseModel `column:"__embedded"`
	Text           *string `column:"text"`
}

func (ExampleTable) TableName() string {
	return "tbl_example"
}

func main() {
	err := xsql.Open(xsql.DbOption{
		Driver:       "postgres",
		DSN:          "postgresql://example:example@localhost:5432/example?sslmode=disable",
		MaxOpenConns: 5,
		MaxIdleTime:  1,
		Dialect:      xsql.PostgreDialect{},
	})
	if err != nil {
		log.Fatalln(err)
	}

	//insert new object
	s := "Item with id = 1"
	example := ExampleTable{
		BaseModel: xsql.BaseModel{
			Id:      1,
			Created: time.Now(),
			Updated: time.Now(),
		},
		Text: &s,
	}
	err = xsql.Insert(example)
	if err != nil {
		log.Fatalln(err)
	}
}
```

> For more tutorial, you can figure out at [example](https://github.com/locngoxuan/xsql/tree/main/example)
