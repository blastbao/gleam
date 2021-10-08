package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

func (d *Dataset) RoundRobin(name string, n int) *Dataset {
	if n <= 1 {
		return d
	}

	shardCount := n * len(d.Shards)

	// 为 flow 创建一个具有 shardSize 个分片的数据集。
	ds := d.Flow.NewNextDataset(shardCount)
	// 为 flow 创建一个类型为 `AllShardTOAllShard` 的 step ，其输入为 d，输出为 ds 。
	step := d.Flow.AddAllToAllStep(d, ds)
	//
	step.SetInstruction(name, instruction.NewRoundRobin())
	return ds
}


// Partition
//
// hash data or by data key, return a new dataset
// This is divided into 2 steps:
// 1. Each record is sharded to a local shard
// 2. The destination shard will collect its child shards and merge into one
func (d *Dataset) Partition(name string, shard int, sortOption *SortOption) *Dataset {

	indexes := sortOption.Indexes()
	if intArrayEquals(d.IsPartitionedBy, indexes) && shard == len(d.Shards) {
		return d
	}

	if 1 == len(d.Shards) && shard == 1 {
		return d
	}

	ret := d.partitionScatter(name, shard, indexes)
	if shard > 1 {
		ret = ret.partitionCollect(name, shard, indexes)
	}

	ret.IsPartitionedBy = indexes
	return ret
}

func (d *Dataset) PartitionByKey(name string, shard int) *Dataset {
	return d.Partition(name, shard, Field(1))
}

func (d *Dataset) partitionScatter(name string, shardCount int, indexes []int) (ret *Dataset) {
	ret = d.Flow.NewNextDataset(len(d.Shards) * shardCount)
	ret.IsPartitionedBy = indexes
	step := d.Flow.AddOneToEveryNStep(d, shardCount, ret)
	step.SetInstruction(name, instruction.NewScatterPartitions(indexes))
	return
}

func (d *Dataset) partitionCollect(name string, shardCount int, indexes []int) (ret *Dataset) {
	ret = d.Flow.NewNextDataset(shardCount)
	ret.IsPartitionedBy = indexes
	step := d.Flow.AddLinkedNToOneStep(d, len(d.Shards)/shardCount, ret)
	step.SetInstruction(name, instruction.NewCollectPartitions())
	return
}

func intArrayEquals(a []int, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
