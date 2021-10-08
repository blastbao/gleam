package instruction

import (
	"fmt"
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(
		func(m *pb.Instruction) Instruction {
			if m.GetLocalGroupBySorted() != nil {
				return NewLocalGroupBySorted(
					toInts(m.GetLocalGroupBySorted().GetIndexes()),
				)
			}
			return nil
		},
	)
}

type LocalGroupBySorted struct {
	indexes []int
}

func NewLocalGroupBySorted(indexes []int) *LocalGroupBySorted {
	return &LocalGroupBySorted{indexes}
}

func (b *LocalGroupBySorted) Name(prefix string) string {
	return prefix + ".LocalGroupBySorted"
}

func (b *LocalGroupBySorted) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoLocalGroupBySorted(readers[0], writers[0], b.indexes, stats)
	}
}

func (b *LocalGroupBySorted) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalGroupBySorted: &pb.Instruction_LocalGroupBySorted{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *LocalGroupBySorted) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

func DoLocalGroupBySorted(reader io.Reader, writer io.Writer, indexes []int, stats *pb.InstructionStat) error {

	var prev util.Row

	err := util.ProcessRow(reader, indexes, func(row *util.Row) error {
		// write prev row if key is different
		stats.InputCounter++

		if prev.K != nil && util.Compare(row.K, prev.K) != 0 {
			// K 发生变化，则将 prev 写入到 writer 中
			if err := prev.WriteTo(writer); err != nil {
				return fmt.Errorf("Sort>Failed to write: %v", err)
			}
			stats.OutputCounter++
			// 更新 prev
			prev.T, prev.K, prev.V = row.T, row.K, []interface{}{row.V}
		} else if prev.K == nil {
			// 初始化 prev
			prev.T, prev.K, prev.V = row.T, row.K, []interface{}{row.V}
		} else {
			// 至此，current row 和 prev row 的 K 相同，进行聚合操作。

			// 更新时间戳，取较大值
			prev.T = max(prev.T, row.T)
			// 聚合 values
			prev.V = append(prev.V, row.V)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("LocalGroupBySorted>Failed:%v\n", err)
		return err
	}

	// 确保没有遗漏的 row
	if err := prev.WriteTo(writer); err != nil {
		return fmt.Errorf("Sort>Failed to write: %v", err)
	}
	stats.OutputCounter++

	return nil

}
