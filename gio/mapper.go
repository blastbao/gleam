package gio

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/chrislusf/gleam/util"
)

func (runner *gleamRunner) processMapper(ctx context.Context, f Mapper) (err error) {
	return runner.report(ctx, func() error {
		return runner.doProcessMapper(ctx, f)
	})
}

func (runner *gleamRunner) doProcessMapper(ctx context.Context, f Mapper) error {
	for {
		// 从标准输入读取数据
		row, err := util.ReadRow(os.Stdin)
		if err != nil {
			// 流关闭，退出
			if err == io.EOF {
				return nil
			}
			// 其它错误，报错
			return fmt.Errorf("mapper input row error: %v", err)
		}
		stat.Stats[0].InputCounter++

		// 格式转换
		var data []interface{}
		data = append(data, row.K...)
		data = append(data, row.V...)

		// 执行 Mapper 函数
		err = f(data)
		if err != nil {
			return fmt.Errorf("processing error: %v", err)
		}
	}
}
