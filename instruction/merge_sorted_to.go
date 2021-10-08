package instruction

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(
		func(m *pb.Instruction) Instruction {
			if m.GetMergeSortedTo() != nil {
				return NewMergeSortedTo(
					toOrderBys(m.GetMergeSortedTo().GetOrderBys()),
				)
			}
			return nil
		},
	)
}

type MergeSortedTo struct {
	orderBys []OrderBy
}

func NewMergeSortedTo(orderBys []OrderBy) *MergeSortedTo {
	return &MergeSortedTo{orderBys}
}

func (b *MergeSortedTo) Name(prefix string) string {
	return prefix + ".MergeSortedTo"
}

func (b *MergeSortedTo) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		// DoMergeSortedTo 多路归并排序，把 n 个 readers 的数据按照 orderBys 进行多路归并排序，然后写出到 writers[0] 中。
		return DoMergeSortedTo(readers, writers[0], b.orderBys, stats)
	}
}

func (b *MergeSortedTo) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		MergeSortedTo: &pb.Instruction_MergeSortedTo{
			OrderBys: getOrderBys(b.orderBys),
		},
	}
}

func (b *MergeSortedTo) GetMemoryCostInMB(partitionSize int64) int64 {
	return 20
}

type rowWithOriginalData struct {
	row       *util.Row
	originalK []interface{}
	originalV []interface{}
}

func newRowWithOriginalData(row *util.Row) *rowWithOriginalData {
	return &rowWithOriginalData{
		row: row,
		originalK: row.K,
		originalV: row.V,
	}
}

func newMinQueueOfRowsWithOriginalData(orderBys []OrderBy) *util.PriorityQueue {
	return util.NewPriorityQueue(func(a, b interface{}) bool {
		x, y := a.(*rowWithOriginalData), b.(*rowWithOriginalData)
		return lessThan(orderBys, x.row, y.row)
	})
}




// DoMergeSortedTo 多路归并排序，把 n 个 readers 的数据按照 orderBys 进行多路归并排序，然后写出到 writers[0] 中。
func DoMergeSortedTo(readers []io.Reader, writer io.Writer, orderBys []OrderBy, stats *pb.InstructionStat) error {

	// 用于排序的字段编号
	indexes := getIndexesFromOrderBys(orderBys)

	// 构造优先级队列，按照原始 row 排序
	pq := newMinQueueOfRowsWithOriginalData(orderBys)





	// enqueue one item to the pq from each channel
	//
	// 遍历 n 个 shards 对应的 readers ，每个 reader 中读取一个 row 来初始化优先级队列
	for shardId, reader := range readers {
		if row, err := util.ReadRow(reader); err == nil {
			rowWithOriginalData := newRowWithOriginalData(row)
			row.UseKeys(indexes)
			stats.InputCounter++
			pq.Enqueue(rowWithOriginalData, shardId)
		} else {
			// 其它错误，报错返回
			if err != io.EOF {
				log.Printf("DoMergeSortedTo failed start :%v", err)
				return err
			}
			// io.EOF ，continue next loop
		}
	}


	for pq.Len() > 0 {
		item, shardId := pq.Dequeue()
		rowWithOriginalData := item.(*rowWithOriginalData)
		rowWithOriginalData.row.K = rowWithOriginalData.originalK
		rowWithOriginalData.row.V = rowWithOriginalData.originalV
		if err := rowWithOriginalData.row.WriteTo(writer); err != nil {
			return err
		}
		stats.OutputCounter++

		if row, err := util.ReadRow(readers[shardId]); err == nil {
			rowWithOriginalData := newRowWithOriginalData(row)
			row.UseKeys(indexes)
			stats.InputCounter++
			pq.Enqueue(rowWithOriginalData, shardId)
		} else {
			if err != io.EOF {
				log.Printf("DoMergeSortedTo failed to ReadRow :%v", err)
				return err
			}
		}
	}
	return nil
}
