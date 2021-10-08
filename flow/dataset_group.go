package flow

import (
	"github.com/chrislusf/gleam/instruction"
)

// GroupBy e.g. GroupBy("", Field(1,2,3)) group data by field 1,2,3
func (d *Dataset) GroupBy(name string, sortOption *SortOption) *Dataset {
	// 排序 + 本地聚合
	ds := d.LocalSort(name, sortOption).LocalGroupBy(name, sortOption)

	// 多路归并
	if len(d.Shards) > 1 {
		ds = ds.MergeSortedTo(name, 1).LocalGroupBy(name, sortOption)
	}

	// OrderBy Conditions
	ds.IsLocalSorted = sortOption.orderByList
	return ds
}

func (d *Dataset) LocalGroupBy(name string, sortOption *SortOption) *Dataset {
	ds, step := add1ShardTo1Step(d)
	ds.IsPartitionedBy = sortOption.Indexes()
	step.SetInstruction(name, instruction.NewLocalGroupBySorted(sortOption.Indexes()))
	return ds
}
