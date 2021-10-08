package instruction

import (
	"io"
	"log"
	"sync"
	"sync/atomic"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(
		func(m *pb.Instruction) Instruction {
			if m.GetRoundRobin() != nil {
				return NewRoundRobin()
			}
			return nil
		},
	)
}

type RoundRobin struct {
}

func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

func (b *RoundRobin) Name(prefix string) string {
	return prefix + ".RoundRobin"
}

func (b *RoundRobin) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		return DoRoundRobin(readers, writers, stats)
	}
}

func (b *RoundRobin) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		RoundRobin: &pb.Instruction_RoundRobin{},
	}
}

func (b *RoundRobin) GetMemoryCostInMB(partitionSize int64) int64 {
	return 1
}

func DoRoundRobin(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	shardCount := int32(len(writers))

	var wg sync.WaitGroup
	count := int32(0)

	// 为每个 reader 启动一个 goroutine ，它从 reader 中读取数据，并按 RoundRobin 算法选择一个 writer 并写入其中。
	for _, reader := range readers {
		wg.Add(1)
		go func(reader io.Reader) {
			err := util.ProcessMessage(reader, func(data []byte) error {
				atomic.AddInt64(&stats.InputCounter, 1)
				atomic.AddInt32(&count, 1)
				err := util.WriteMessage(writers[count%shardCount], data)
				if err == nil {
					atomic.AddInt64(&stats.OutputCounter, 1)
				}
				return err
			})
			if err != nil {
				log.Println(err)
			}
			wg.Done()
		}(reader)
	}
	wg.Wait()
	return nil
}
