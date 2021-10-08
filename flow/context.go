// Package flow contains data structure for computation.
// Mostly Dataset operations such as Map/Reduce/Join/Sort etc.
package flow

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/util"
)



// New 创建一个 flow
func New(name string) (fc *Flow) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	fc = &Flow{
		Name:     name,			// 流名
		HashCode: r.Uint32(),	// 随机数
	}
	return
}

func (flow *Flow) Run(options ...FlowOption) {
	flow.RunContext(context.Background(), options...)
}

func (flow *Flow) RunContext(ctx context.Context, options ...FlowOption) {
	// ???
	if !gio.Initialized {
		println("gio.Init() is required right after main() if pure go mapper or reducer is used.")
		os.Exit(1)
	}

	//
	if len(options) == 0 {
		Local.RunFlow(ctx, flow)
	} else {
		for _, option := range options {
			option.GetFlowRunner().RunFlow(ctx, flow)
		}
	}
}

// NewNextDataset 为 flow 创建一个具有 shardSize 个分片的数据集。
func (flow *Flow) NewNextDataset(shardSize int) (dataSet *Dataset) {
	// 创建新数据集 dataSet ，并将其添加到 flow.Datasets 中
	dataSet = newDataset(flow)
	// 为数据集 dataSet 创建 shardSize 个子分片
	setupDatasetShard(dataSet, shardSize)
	return
}

// AddOneToOneStep the tasks should run on the source dataset shard
//
//
func (flow *Flow) AddOneToOneStep(input *Dataset, output *Dataset) (step *Step) {

	// 为 flow 创建一个类型为 `OneShardToOneShard` 的 step 。
	step = flow.NewStep()
	step.NetworkType = OneShardToOneShard

	// 绑定 step 和 output dataset
	fromStepToDataset(step, output)
	// 绑定 step 和 input dataset
	fromDatasetToStep(input, step)

	// 如果输入为空
	if input == nil {
		// 为 step 创建一个 task
		task := step.NewTask()
		// 如果输出非空，就绑定输出数据集的第一个分片为 task 的输出分片
		if output != nil && output.Shards != nil {
			fromTaskToDatasetShard(task, output.GetShards()[0])
		}
		return
	}


	// setup the network
	//
	//
	for i, shard := range input.GetShards() {
		// 为 step 创建一个 task
		task := step.NewTask()
		//
		if output != nil && output.Shards != nil {
			fromTaskToDatasetShard(task, output.GetShards()[i])
		}
		//
		fromDatasetShardToTask(shard, task)
	}

	// 返回
	return
}

// AddAllToOneStep the task should run on the destination dataset shard
func (flow *Flow) AddAllToOneStep(input *Dataset, output *Dataset) (step *Step) {

	// 为 flow 创建一个类型为 `AllShardToOneShard` 的 step 。
	step = flow.NewStep()
	step.NetworkType = AllShardToOneShard

	// 绑定 step 和 output dataset
	fromStepToDataset(step, output)
	// 绑定 step 和 input dataset
	fromDatasetToStep(input, step)

	// setup the network

	// 为 step 创建一个 task
	task := step.NewTask()


	// 有一个输出分片
	if output != nil {
		fromTaskToDatasetShard(task, output.GetShards()[0])
	}

	// 有 n 个输入分片
	for _, shard := range input.GetShards() {
		fromDatasetShardToTask(shard, task)
	}

	return
}

// AddOneToAllStep the task should run on the source dataset shard
// input is nil for initial source dataset
func (flow *Flow) AddOneToAllStep(input *Dataset, output *Dataset) (step *Step) {
	step = flow.NewStep()
	step.NetworkType = OneShardToAllShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network

	// 为 step 创建一个 task
	task := step.NewTask()


	// 有一个输入分片
	if input != nil {
		fromDatasetShardToTask(input.GetShards()[0], task)
	}

	// 有 n 个输出分片
	for _, shard := range output.GetShards() {
		fromTaskToDatasetShard(task, shard)
	}

	return
}

func (flow *Flow) AddAllToAllStep(input *Dataset, output *Dataset) (step *Step) {

	// 为 flow 创建一个类型为 `AllShardTOAllShard` 的 step 。
	step = flow.NewStep()
	step.NetworkType = AllShardTOAllShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network

	// 为 step 创建一个 task
	task := step.NewTask()

	// task 有 n 个输入分片
	for _, shard := range input.GetShards() {
		fromDatasetShardToTask(shard, task)
	}

	// task 有 n 个输出分片
	for _, shard := range output.GetShards() {
		fromTaskToDatasetShard(task, shard)
	}

	return
}

func (flow *Flow) AddOneToEveryNStep(input *Dataset, n int, output *Dataset) (step *Step) {

	// 为 flow 创建一个类型为 `OneShardToEveryNShard` 的 step 。
	step = flow.NewStep()
	step.NetworkType = OneShardToEveryNShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	m := len(input.GetShards())
	for i, inShard := range input.GetShards() {
		// 为 step 创建一个 task
		task := step.NewTask()
		// 为 task 绑定 n 个输出分片
		for k := 0; k < n; k++ {
			fromTaskToDatasetShard(task, output.GetShards()[k*m+i])
		}
		// 为 task 绑定一个输入分片
		fromDatasetShardToTask(inShard, task)
	}
	return
}

func (flow *Flow) AddLinkedNToOneStep(input *Dataset, m int, output *Dataset) (step *Step) {
	step = flow.NewStep()
	step.NetworkType = LinkedNShardToOneShard
	fromStepToDataset(step, output)
	fromDatasetToStep(input, step)

	// setup the network
	for i, outShard := range output.GetShards() {
		task := step.NewTask()
		fromTaskToDatasetShard(task, outShard)
		for k := 0; k < m; k++ {
			fromDatasetShardToTask(input.GetShards()[i*m+k], task)
		}
	}
	return
}

// MergeDatasets1ShardTo1Step All dataset should have the same number of shards.
func (flow *Flow) MergeDatasets1ShardTo1Step(inputs []*Dataset, output *Dataset) (step *Step) {

	step = flow.NewStep()
	step.NetworkType = MergeTwoShardToOneShard
	fromStepToDataset(step, output)
	for _, input := range inputs {
		fromDatasetToStep(input, step)
	}

	// setup the network
	if output != nil {
		for shardId, outShard := range output.Shards {
			task := step.NewTask()
			for _, input := range inputs {
				fromDatasetShardToTask(input.GetShards()[shardId], task)
			}
			fromTaskToDatasetShard(task, outShard)
		}
	}
	return
}

// 绑定 step 和 output dataset
func fromStepToDataset(step *Step, output *Dataset) {
	if output == nil {
		return
	}
	// 正向关系: step 1:1 output dataset
	step.OutputDataset = output
	// 反向关系: output dataset 1:1 step
	output.Step = step
}

// 绑定 step 和 input dataset
func fromDatasetToStep(input *Dataset, step *Step) {
	if input == nil {
		return
	}
	// 正向关系: step 1:n input dataset
	step.InputDatasets = append(step.InputDatasets, input)
	// 反向关系: input step 1:n step
	input.ReadingSteps = append(input.ReadingSteps, step)
}

// 为数据集 d 创建 n 个分片
func setupDatasetShard(d *Dataset, n int) {
	// 创建并保存 n 个分片
	for i := 0; i < n; i++ {
		// 新建数据分片
		ds := &DatasetShard{
			Id:           i,				// 分片 ID
			Dataset:      d,				// 归属数据集
			IncomingChan: util.NewPiper(),	// 输入管道
		}
		// 保存分片
		d.Shards = append(d.Shards, ds)
	}
}

// 为 task 绑定输入分片
func fromDatasetShardToTask(shard *DatasetShard, task *Task) {
	// 构造 pipe
	piper := util.NewPiper()

	//
	shard.ReadingTasks = append(shard.ReadingTasks, task)
	// 保存输出管道
	shard.OutgoingChans = append(shard.OutgoingChans, piper)

	// 保存输入分片
	task.InputShards = append(task.InputShards, shard)
	// 保存输入管道
	task.InputChs = append(task.InputChs, piper)
}

// 为 task 绑定输出分片
func fromTaskToDatasetShard(task *Task, shard *DatasetShard) {
	if shard != nil {
		task.OutputShards = append(task.OutputShards, shard)
	}
}
