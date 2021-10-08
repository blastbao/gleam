package flow

import (
	"context"
	"fmt"
	"time"
)

func newDataset(flow *Flow) *Dataset {
	// 新建 Dataset
	d := &Dataset{
		Id:   len(flow.Datasets), // 递增数据集 ID
		Flow: flow,               //
		Meta: &DatesetMetadata{ // 元数据
			TotalSize: -1,
		},
	}
	// 保存 Dataset
	flow.Datasets = append(flow.Datasets, d)
	return d
}

func (d *Dataset) GetShards() []*DatasetShard {
	return d.Shards
}

// Run starts the whole flow. This is a convenient method, same as *Flow.Run()
func (d *Dataset) Run(option ...FlowOption) {
	d.RunContext(context.Background(), option...)
}

// RunContext starts the whole flow. This is a convenient method, same as *Flow.RunContext()
func (d *Dataset) RunContext(ctx context.Context, option ...FlowOption) {
	d.Flow.RunContext(ctx, option...)
}

// Closed 是否已关闭
func (s *DatasetShard) Closed() bool {
	return !s.CloseTime.IsZero()
}

// TimeTaken 耗时
func (s *DatasetShard) TimeTaken() time.Duration {
	if s.Closed() {
		return s.CloseTime.Sub(s.ReadyTime)
	}
	return time.Now().Sub(s.ReadyTime)
}

// Name 数据集分片名
func (s *DatasetShard) Name() string {
	return fmt.Sprintf("f%d-d%d-s%d", s.Dataset.Flow.HashCode, s.Dataset.Id, s.Id)
}
