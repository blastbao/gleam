//go:generate truepack -fast-strings -o row_codec.go
package util

type Row struct {
	K []interface{} `msg:"K"`
	V []interface{} `msg:"V"`
	T int64         `msg:"T"`
}

func NewRow(timestamp int64, objects ...interface{}) *Row {
	r := &Row{
		T: timestamp,
	}
	if len(objects) > 0 {
		r.AppendKey(objects[0]).AppendValue(objects[1:]...)
	}
	return r
}

func (row *Row) AppendKey(keys ...interface{}) *Row {
	row.K = append(row.K, keys...)
	return row
}

func (row *Row) AppendValue(values ...interface{}) *Row {
	row.V = append(row.V, values...)
	return row
}

// UseKeys use the indexes[] specified fields as key fields and the rest of fields as value fields
//
// UseKeys 将 indexes[] 指定的字段用作 key ，其余字段用作 value 。
func (row *Row) UseKeys(indexes []int) (err error) {

	if indexes == nil {
		return nil
	}

	var keys, values []interface{}
	kLen, vLen := len(row.K), len(row.V)

	used := make([]bool, kLen+vLen)

	//
	for _, x := range indexes {
		if x <= kLen {
			keys = append(keys, row.K[x-1])
		} else {
			keys = append(keys, row.V[x-1-kLen])
		}
		used[x-1] = true
	}

	for i, key := range row.K {
		if !used[i] {
			values = append(values, key)
		}
	}

	for i, value := range row.V {
		if !used[i+kLen] {
			values = append(values, value)
		}
	}

	row.K, row.V = keys, values
	return err
}
