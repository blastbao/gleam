package flow

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/chrislusf/gleam/util/on_interrupt"
)

type FlowRunner interface {
	RunFlow(context.Context, *Flow)
}

type FlowOption interface {
	GetFlowRunner() FlowRunner
}

type localDriver struct {
	ctx context.Context
}

var (
	Local *localDriver
)

func init() {
	Local = &localDriver{}
}

func (r *localDriver) GetFlowRunner() FlowRunner {
	return r
}

func (r *localDriver) RunFlow(ctx context.Context, fc *Flow) {
	r.ctx = ctx

	var wg sync.WaitGroup
	wg.Add(1)
	r.RunFlowAsync(&wg, fc)
	wg.Wait()
}

func (r *localDriver) RunFlowAsync(wg *sync.WaitGroup, fc *Flow) {
	defer wg.Done()

	// 如果有中断信号到达，就 os.Exit(0) 退出进程。
	on_interrupt.OnInterrupt(fc.OnInterrupt, nil)

	// 遍历 steps
	for _, step := range fc.Steps {
		// 如果输出数据集为空，意味着 step 尚未执行，就启动协程执行 step ，这些 step 没有依赖关系，可以并发执行。
		if step.OutputDataset == nil {
			wg.Add(1)
			go func(step *Step) {
				r.runStep(wg, step)
			}(step)
		}
	}
}

//
//
func (r *localDriver) runDataset(wg *sync.WaitGroup, d *Dataset) {
	defer wg.Done()

	// 数据集加锁
	d.Lock()
	defer d.Unlock()

	// 禁止重复启动
	if !d.StartTime.IsZero() {
		return
	}
	d.StartTime = time.Now()

	// 遍历每个数据集分片
	for _, shard := range d.Shards {
		wg.Add(1)
		go func(shard *DatasetShard) {
			// 执行数据集分片
			r.runDatasetShard(wg, shard)
		}(shard)
	}

	wg.Add(1)

	// 执行 step
	r.runStep(wg, d.Step)
}

func (r *localDriver) runDatasetShard(wg *sync.WaitGroup, shard *DatasetShard) {
	defer wg.Done()

	// 设置启动时间
	shard.ReadyTime = time.Now()

	// 获取 writer 列表
	var writers []io.Writer
	for _, outgoingChan := range shard.OutgoingChans {
		writers = append(writers, outgoingChan.Writer)
	}

	// 数据传输
	util.BufWrites(writers, func(writers []io.Writer) {
		// 合并多个 writers
		w := io.MultiWriter(writers...)

		// 把数据从 shard.IncomingChan.Reader 拷贝到 w 中
		n, _ := io.Copy(w, shard.IncomingChan.Reader)
		// println("shard", shard.Name(), "moved", n, "bytes.")

		// 保存传输的字节数
		shard.Counter = n

		// 保存传输结束的时间戳
		shard.CloseTime = time.Now()
	})

	// 关闭 writers
	for _, outgoingChan := range shard.OutgoingChans {
		outgoingChan.Writer.Close()
	}
}

func (r *localDriver) runStep(wg *sync.WaitGroup, step *Step) {
	defer wg.Done()

	// 执行过程中，为 step 加锁
	step.Lock()
	defer step.Unlock()

	// 不许重复启动
	if !step.StartTime.IsZero() {
		return
	}

	// 设置启动时间
	step.StartTime = time.Now()

	// 逐个启动 step 的每个任务，这些任务可以并发执行。
	for _, task := range step.Tasks {
		wg.Add(1)
		go func(task *Task) {
			r.runTask(wg, task)
		}(task)
	}

	// 逐个启动 step 的输入数据集，这些数据集可以并发执行。
	for _, ds := range step.InputDatasets {
		wg.Add(1)
		go func(ds *Dataset) {
			r.runDataset(wg, ds)
		}(ds)
	}

}

func (r *localDriver) runTask(wg *sync.WaitGroup, task *Task) {
	defer wg.Done()

	// try to run Function first, if failed, try to run shell scripts
	// 如果 Function 非空，就执行它，否则执行 shell 脚本。

	if task.Step.Function != nil {
		// each function should close its own Piper output writer and close it's own Piper input reader
		task.Step.RunFunction(task)
		return
	}

	// get an exec.Command
	scriptCommand := task.Step.GetScriptCommand()
	execCommand := scriptCommand.ToOsExecCommand()

	if task.Step.NetworkType == OneShardToOneShard {
		// fmt.Printf("execCommand: %+v\n", execCommand)
		reader := task.InputChs[0].Reader
		writer := task.OutputShards[0].IncomingChan.Writer
		wg.Add(1)
		prevIsPipe := task.InputShards[0].Dataset.Step.IsPipe
		task.Stat = &pb.InstructionStat{}
		util.Execute(r.ctx, wg, task.Stat, task.Step.Name, execCommand, reader, writer, prevIsPipe, task.Step.IsPipe, true, os.Stderr)
	} else {
		println("network type:", task.Step.NetworkType)
	}

}
