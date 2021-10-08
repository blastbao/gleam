package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(
		func(m *pb.Instruction) Instruction {
			if m.GetCollectPartitions() != nil {
				return NewCollectPartitions()
			}
			return nil
		},
	)
}

type CollectPartitions struct {
}

func NewCollectPartitions() *CollectPartitions {
	return &CollectPartitions{}
}

func (b *CollectPartitions) Name(prefix string) string {
	return prefix + ".CollectPartitions"
}

func (b *CollectPartitions) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoCollectPartitions(readers, writers[0], stats)
	}
}

func (b *CollectPartitions) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		CollectPartitions: &pb.Instruction_CollectPartitions{},
	}
}

func (b *CollectPartitions) GetMemoryCostInMB(partitionSize int64) int64 {
	return 3
}

func DoCollectPartitions(readers []io.Reader, writer io.Writer, stats *pb.InstructionStat) (err error) {

	// 如果只有一个 reader ，意味着只有一个 partition ，就直接将该 reader 转发到 writer 上。
	if len(readers) == 1 {
		n, err := io.Copy(writer, readers[0])
		stats.InputCounter, stats.OutputCounter = n, n
		return err
	}

	// 如果有多个 readers ，意味着有多个 partitions ，则把这些 readers 的数据汇总到 writer 上。
	stats.InputCounter, stats.OutputCounter, err = util.CopyMultipleReaders(readers, writer)
	return
}
