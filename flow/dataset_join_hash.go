package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// HashJoin joins two datasets by putting the smaller dataset in memory on all
// executors and streams through the bigger dataset.
func (d *Dataset) HashJoin(name string, smaller *Dataset, sortOption *SortOption) *Dataset {
	return smaller.Broadcast(name, len(d.Shards)).LocalHashAndJoinWith(name, d, sortOption)
}

func (d *Dataset) LocalHashAndJoinWith(name string, that *Dataset, sortOption *SortOption) *Dataset {
	ret := d.Flow.NewNextDataset(len(that.Shards))
	ret.IsPartitionedBy = that.IsPartitionedBy
	ret.IsLocalSorted = that.IsLocalSorted
	inputs := []*Dataset{d, that}
	step := d.Flow.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(name, instruction.NewLocalHashAndJoinWith(sortOption.Indexes()))
	return ret
}

// Broadcast replicates itself to all shards.
func (d *Dataset) Broadcast(name string, shardCount int) *Dataset {
	if shardCount == 1 && len(d.Shards) == shardCount {
		return d
	}
	ret := d.Flow.NewNextDataset(shardCount)
	step := d.Flow.AddOneToAllStep(d, ret)
	step.SetInstruction(name, instruction.NewBroadcast())
	return ret
}
