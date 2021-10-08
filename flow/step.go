package flow

import (
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
)


// NewStep 为 flow 创建一个新的 step 。
func (flow *Flow) NewStep() (step *Step) {
	// 创建一个 step
	step = &Step{
		Id:     len(flow.Steps),              // ID++
		Params: make(map[string]interface{}), // 参数列表
		Meta: &StepMetadata{ // 元数据
			IsIdempotent: true,
		},
	}
	// 将 step 保存到 flow.Steps 上
	flow.Steps = append(flow.Steps, step)
	return
}

// NewTask 为 step 创建一个 task 。
func (step *Step) NewTask() (task *Task) {
	// 创建一个 task
	task = &Task{
		Step: step,	// 反向引用
		Id: len(step.Tasks),
	}
	// 将 task 保存到 step.Tasks 上
	step.Tasks = append(step.Tasks, task) // 正向引用
	return
}

func (step *Step) SetInstruction(prefix string, ins instruction.Instruction) {
	step.Name = ins.Name(prefix)
	step.Function = ins.Function()
	step.Instruction = ins
}

func (step *Step) RunFunction(task *Task) error {
	var readers []io.Reader
	var writers []io.Writer

	// Readers
	for i, reader := range task.InputChs {
		var r io.Reader = reader.Reader
		if task.InputShards[i].Dataset.Step.IsPipe {
			r = util.ConvertLineReaderToRowReader(r, step.Name, os.Stderr)
		}
		readers = append(readers, r)
	}

	// Writers
	for _, shard := range task.OutputShards {
		writers = append(writers, shard.IncomingChan.Writer)
	}

	// Stats
	if task.Stat == nil {
		task.Stat = &pb.InstructionStat{}
	}

	// 调用 step 的指令函数 func([]io.Reader, []io.Writer, *pb.InstructionStat) error
	err := task.Step.Function(readers, writers, task.Stat)
	if err != nil {
		log.Printf("Failed to run task %s-%d: %v\n", task.Step.Name, task.Id, err)
	}

	// 逐个关闭 Writers
	for _, writer := range writers {
		if c, ok := writer.(io.Closer); ok {
			c.Close()
		}
	}

	return err
}

func (step *Step) GetScriptCommand() *script.Command {
	if step.Command == nil {
		return step.Script.GetCommand()
	}
	return step.Command
}
