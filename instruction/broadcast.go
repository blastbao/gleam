package instruction

import (
	"io"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(
		func(m *pb.Instruction) Instruction {
			if m.GetBroadcast() != nil {
				return NewBroadcast()
			}
			return nil
		},
	)
}

type Broadcast struct {
}

func NewBroadcast() *Broadcast {
	return &Broadcast{}
}

func (b *Broadcast) Name(prefix string) string {
	return prefix + ".Broadcast"
}

func (b *Broadcast) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		// 从 readers[0] 中读取消息，广播给 writers ，同时更新统计计数 stats 。
		return DoBroadcast(readers[0], writers, stats)
	}
}

func (b *Broadcast) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		Broadcast: &pb.Instruction_Broadcast{},
	}
}

func (b *Broadcast) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoBroadcast(reader io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return util.ProcessMessage(
		reader,
		func(data []byte) error {
			// 读计数
			stats.InputCounter++
			for _, writer := range writers {
				// 写计数
				stats.OutputCounter++
				// 把 data 写入到 writer 中
				_ = util.WriteMessage(writer, data)
			}
			return nil
		},
	)
}
