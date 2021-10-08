package mapper

import (
	"strings"

	"github.com/chrislusf/gleam/gio"
)

var (
	Tokenize  = gio.RegisterMapper(tokenize)
	AppendOne = gio.RegisterMapper(addOne)
)

func tokenize(row []interface{}) error {
	// 格式转换
	line := gio.ToString(row[0])
	// 提取字符串
	for _, field := range strings.FieldsFunc(line, func(r rune) bool {
		return !('A' <= r && r <= 'Z' || 'a' <= r && r <= 'z' || '0' <= r && r <= '9')
	}) {
		// 构造一个 row 并写入到 std.out
		gio.Emit(field)
	}
	return nil
}

func addOne(row []interface{}) error {
	row = append(row, 1)
	gio.Emit(row...)
	return nil
}
