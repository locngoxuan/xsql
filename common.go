package xsql

import (
	"reflect"
	"strings"
)

func strRepeat(begin, end, pattern, sep string, size int) string {
	if size <= 0 {
		return ""
	}
	var b strings.Builder
	defer b.Reset()
	if begin != "" {
		b.WriteString(begin)
	}
	b.WriteString(pattern)
	for i := 0; i < size-1; i++ {
		b.WriteString(sep)
		b.WriteString(pattern)
	}
	if end != "" {
		b.WriteString(end)
	}
	return b.String()
}

func recursiveScan(v reflect.Type, fields map[string]string) {
	for i := 0; i < v.NumField(); i++ {
		column := v.Field(i).Tag.Get("column")
		if column == "-" {
			continue
		}

		if column == "__embedded" {
			if v.Field(i).Type.Kind() == reflect.Struct {
				recursiveScan(v.Field(i).Type, fields)
			} else if v.Field(i).Type.Kind() == reflect.Ptr {
				recursiveScan(v.Field(i).Type.Elem(), fields)
			}
			continue
		}

		fieldName := v.Field(i).Name
		if column == "" {
			column = fieldName
		}

		fields[column] = fieldName
	}
}

func getTableInfo(valType reflect.Type) ([]string, []string) {
	if valType.Kind() == reflect.Ptr {
		valType = valType.Elem()
	}
	m := make(map[string]string)
	recursiveScan(valType, m)
	columns := make([]string, len(m))
	fieldNames := make([]string, len(m))
	i := 0
	for col, field := range m {
		columns[i] = col
		fieldNames[i] = field
		i++
	}
	return columns, fieldNames
}

func getTableName(val reflect.Value) string {
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	m, ok := val.Type().MethodByName("TableName")
	if !ok {
		return val.Type().Name()
	} else {
		v := m.Func.Call([]reflect.Value{val})
		return v[0].String()
	}
}

func strToIntf(s []string) []interface{} {
	b := make([]interface{}, len(s))
	for i, v := range s {
		b[i] = v
	}
	return b
}

func chunk(list reflect.Value, size int) [][]reflect.Value {
	rs := make([][]reflect.Value, 0)
	totalItem := list.Len()
	remaining := totalItem
	lastIndex := 0
	for remaining > 0 {
		thisBatchSize := remaining
		if remaining > size {
			thisBatchSize = size
		}
		remaining = remaining - thisBatchSize
		batch := make([]reflect.Value, thisBatchSize)
		for i := 0; i < thisBatchSize; i++ {
			batch[i] = list.Index(i + lastIndex)
		}
		lastIndex += thisBatchSize
		rs = append(rs, batch)
	}
	return rs
}
