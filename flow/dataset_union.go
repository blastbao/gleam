package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// Union union multiple Datasets as one Dataset
func (d *Dataset) Union(name string, others []*Dataset, isParallel bool) *Dataset {
	ret := d.Flow.NewNextDataset(len(d.Shards))
	inputs := []*Dataset{d}
	inputs = append(inputs, others...)
	step := d.Flow.MergeDatasets1ShardTo1Step(inputs, ret)
	step.SetInstruction(name, instruction.NewUnion(isParallel))
	return ret
}
