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
			//
			if m.GetLocalTop() != nil {
				return NewLocalTop(
					int(m.GetLocalTop().GetN()),
					toOrderBys(m.GetLocalTop().GetOrderBys()),
				)
			}
			return nil
		},
	)
}

type LocalTop struct {
	n        int			// top n
	orderBys []OrderBy		// order by
}

func NewLocalTop(n int, orderBys []OrderBy) *LocalTop {
	return &LocalTop{n, orderBys}
}

func (b *LocalTop) Name(prefix string) string {
	return prefix + ".LocalTop"
}

func (b *LocalTop) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		// 从 readers[0] 中读取 rows ，进行排序，然后取出 top n 写出到 writers[0] 中。
		return DoLocalTop(readers[0], writers[0], b.n, b.orderBys, stats)
	}
}

func (b *LocalTop) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalTop: &pb.Instruction_LocalTop{
			N:        int32(b.n),
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *LocalTop) GetMemoryCostInMB(partitionSize int64) int64 {
	return 5
}

// DoLocalTop streamingly compare and get the top n items
func DoLocalTop(reader io.Reader, writer io.Writer, n int, orderBys []OrderBy, stats *pb.InstructionStat) error {

	// 构造优先级队列
	pq := newMinQueueOfPairs(orderBys)

	err := util.ProcessRow(reader, nil, func(row *util.Row) error {
		stats.InputCounter++
		if pq.Len() >= n {
			// 如果 pq.Top() < row ，则入堆
			if lessThan(orderBys, pq.Top().(*util.Row), row) {
				pq.Dequeue()
				pq.Enqueue(row, 0)
			}
		} else {
			pq.Enqueue(row, 0)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Top>Failed to process input data:%v\n", err)
		return err
	}

	// read data out of the priority queue
	//
	// 从优先级队列中取 top n 元素
	length := pq.Len()
	itemsToReverse := make([]*util.Row, length)
	for i := 0; i < length; i++ {
		entry, _ := pq.Dequeue()
		itemsToReverse[i] = entry.(*util.Row)
	}

	// 按逆序将 top n 元素写入到 writer 中
	for i := length - 1; i >= 0; i-- {
		itemsToReverse[i].WriteTo(writer)
		stats.OutputCounter++
	}

	return nil
}

// 构造优先级队列，值越小则优先级越高。
func newMinQueueOfPairs(orderBys []OrderBy) *util.PriorityQueue {
	return util.NewPriorityQueue(func(a, b interface{}) bool {
		x, y := a.(*util.Row), b.(*util.Row)
		return lessThan(orderBys, x, y)
	})
}
