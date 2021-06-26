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
	{
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

	//insert batch
	{
		var examples []ExampleTable
		for i := 0; i < 100; i++ {
			s := fmt.Sprintf("Item with id = %d", i+2)
			examples = append(examples, ExampleTable{
				BaseModel: xsql.BaseModel{
					Id:      int64(i + 2),
					Created: time.Now(),
					Updated: time.Now(),
				},
				Text: &s,
			})
		}
		err = xsql.InsertBatch(examples, 10)
		if err != nil {
			log.Fatalln(err)
		}
	}

	//count
	{
		i, err := xsql.Count(ExampleTable{})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of records in tbl_example is %d", i)
	}

	//update
	{
		i, err := xsql.Update(xsql.NewStmt(`UPDATE tbl_example SET text = :text`).
			AppendSql(`WHERE id IN (:ids)`).
			With(map[string]interface{}{
				"ids":  []int{1, 2, 3, 4},
				"text": nil,
			}).
			ExpectedResult(4).
			Get())
		if err != nil {
			log.Fatalln(err, i)
		}
		var rs []ExampleTable
		err = xsql.Query(xsql.NewStmt(`SELECT * FROM tbl_example WHERE id IN (:ids)`).
			With(map[string]interface{}{
				"ids": []int{1, 2, 3, 4},
			}).Get(), &rs)
		if err != nil {
			log.Fatalln(err)
		}
		for _, e := range rs {
			log.Println(e)
		}
	}

	//transaction
	{
		tx, err := xsql.BeginTx()
		if err != nil {
			log.Fatalln(err)
		}
		i, err := xsql.DeleteTx(tx, xsql.NewStmt(`DELETE FROM tbl_example WHERE id % 2 = 0`).Get())
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of deleted records is %d", i)
		i, err = xsql.CountTx(tx, ExampleTable{})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of records within transaction is %d", i)
		i, err = xsql.Count(ExampleTable{})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of records in tbl_example is %d", i)
		//delete all
		i, err = xsql.DeleteTx(tx, xsql.NewStmt(`DELETE FROM tbl_example WHERE 1=1`).Get())
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of deleted records is %d", i)
		i, err = xsql.CountTx(tx, ExampleTable{})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of records within transaction is %d", i)
		_ = tx.Commit()
		i, err = xsql.Count(ExampleTable{})
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("number of records in tbl_example is %d", i)
	}
}
