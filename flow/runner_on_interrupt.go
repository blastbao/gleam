package flow

import (
	"fmt"
)

func (flow *Flow) OnInterrupt() {

	fmt.Print("\n")

	// 遍历 steps
	for _, step := range flow.Steps {

		// 如果 step 输出数据集非空
		if step.OutputDataset != nil {
			fmt.Printf("step:%s%d\n", step.Name, step.Id)

			// 遍历 step 的输入数据集
			for _, input := range step.InputDatasets {
				fmt.Printf("  input  : d%d\n", input.Id)
				// 遍历 step 的数据数据集的每个分片
				for _, shard := range input.Shards {
					// 打印数据集分片状态
					printShardStatus(shard)
				}
			}
			fmt.Printf("  output : d%d\n", step.OutputDataset.Id)

			// 遍历 step 的任务
			for _, task := range step.Tasks {
				// 遍历每个任务的输出数据分片
				for _, shard := range task.OutputShards {
					// 打印数据集分片状态
					printShardStatus(shard)
				}
			}
		}
	}
	fmt.Print("\n")
}

func printShardStatus(shard *DatasetShard) {
	if shard.Closed() {
		fmt.Printf("     shard:%d time:%v completed %d\n", shard.Id, shard.TimeTaken(), shard.Counter)
	} else {
		fmt.Printf("     shard:%d time:%v processed %d\n", shard.Id, shard.TimeTaken(), shard.Counter)
	}
}
