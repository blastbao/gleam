package instruction

import (
	"fmt"
	"io"
	"io/ioutil"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

func init() {
	InstructionRunner.Register(
		func(m *pb.Instruction) Instruction {
			if m.GetLocalHashAndJoinWith() != nil {
				return NewLocalHashAndJoinWith(
					toInts(m.GetLocalHashAndJoinWith().GetIndexes()),
				)
			}
			return nil
		},
	)
}

type LocalHashAndJoinWith struct {
	indexes []int
}

func NewLocalHashAndJoinWith(indexes []int) *LocalHashAndJoinWith {
	return &LocalHashAndJoinWith{indexes}
}

func (b *LocalHashAndJoinWith) Name(prefix string) string {
	return prefix + ".LocalHashAndJoinWith"
}

func (b *LocalHashAndJoinWith) Function() func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
	return func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {

		// 从 readers[0] 中读取 rows ，并把每个 row 的 pair<key,*row> 保存在 hashmap 中。
		// 从 readers[1] 中读取 rows ，如果某个 row 的 key 存在于 hashmap ，
		// 就合并 row 和 hashmap[key].row 的 values ，然后写入到 writer 中。
		return DoLocalHashAndJoinWith(readers[0], readers[1], writers[0], b.indexes, stats)
	}
}

func (b *LocalHashAndJoinWith) SerializeToCommand() *pb.Instruction {
	return &pb.Instruction{
		LocalHashAndJoinWith: &pb.Instruction_LocalHashAndJoinWith{
			Indexes: getIndexes(b.indexes),
		},
	}
}

func (b *LocalHashAndJoinWith) GetMemoryCostInMB(partitionSize int64) int64 {
	return int64(float32(partitionSize) * 1.1)
}

func DoLocalHashAndJoinWith(leftReader, rightReader io.Reader, writer io.Writer, indexes []int, stats *pb.InstructionStat) error {

	hashmap := make(map[string]*util.Row)

	// 从 leftReader 中读取 rows ，并把每个 row 的 pair<key,*row> 保存在 hashmap 中。
	err := util.ProcessRow(leftReader, indexes, func(row *util.Row) error {
		// write the row if key is different
		stats.InputCounter++
		keyBytes, _ := util.EncodeKeys(row.K...)
		hashmap[string(keyBytes)] = row
		return nil
	})
	if err != nil {
		fmt.Printf("Sort>Failed to read input data:%v\n", err)
		return err
	}

	if len(hashmap) == 0 {
		io.Copy(ioutil.Discard, rightReader)
		return nil
	}

	// 从 rightReader 中读取 rows ，如果某个 row 的 key 存在于 hashmap ，
	// 那么就合并 row 和 hashmap[key].row 的 values ，然后写入到 writer 中，。
	err = util.ProcessRow(rightReader, indexes, func(row *util.Row) error {
		// write the row if key is different
		stats.InputCounter++
		keyBytes, err := util.EncodeKeys(row.K...)
		if err != nil {
			return fmt.Errorf("Failed to encoded keys %+v: %v", row.K, err)
		}

		if mappedRow, ok := hashmap[string(keyBytes)]; ok {
			row.AppendValue(mappedRow.V...).WriteTo(writer)
			stats.OutputCounter++
		}
		return nil
	})


	if err != nil {
		fmt.Printf("LocalHashAndJoinWith>Failed to process the bigger input data:%v\n", err)
	}


	return err
}
