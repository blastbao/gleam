package flow

import (
	"io"
	"sync"
	"time"

	"github.com/chrislusf/gleam/instruction"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/script"
	"github.com/chrislusf/gleam/util"
)

type NetworkType int

const (
	OneShardToOneShard NetworkType = iota
	OneShardToAllShard
	AllShardToOneShard
	OneShardToEveryNShard
	LinkedNShardToOneShard
	MergeTwoShardToOneShard
	AllShardTOAllShard
)

type DatasetShardStatus int

const (
	Untouched DatasetShardStatus = iota
	LocationAssigned
	InProgress
	InRetry
	Failed
	Successful
)

type ModeIO int

const (
	ModeInMemory ModeIO = iota
	ModeOnDisk
)

type DatesetMetadata struct {
	TotalSize int64
	OnDisk    ModeIO
}

type DatesetShardMetadata struct {
	TotalSize int64
	Timestamp time.Time
	URI       string
	Status    DatasetShardStatus
	Error     error
}

type StepMetadata struct {
	IsRestartable bool
	IsIdempotent  bool
}

type Flow struct {
	Name     string		// 名称
	Steps    []*Step	// 步骤
	Datasets []*Dataset	// 数据集
	HashCode uint32		// 哈希码
}

type Dataset struct {
	Flow            *Flow
	Id              int
	Shards          []*DatasetShard
	Step            *Step
	ReadingSteps    []*Step
	IsPartitionedBy []int
	IsLocalSorted   []instruction.OrderBy	//
	Meta            *DatesetMetadata
	RunLocked
}

type DatasetShard struct {
	Id            int
	Dataset       *Dataset
	ReadingTasks  []*Task
	IncomingChan  *util.Piper
	OutgoingChans []*util.Piper
	Counter       int64
	ReadyTime     time.Time
	CloseTime     time.Time
	Meta          *DatesetShardMetadata
}

type Step struct {
	Id             int
	// 流
	Flow           *Flow
	// 输入数据集
	InputDatasets  []*Dataset
	// 输出数据集
	OutputDataset  *Dataset

	// 名称
	Name           string
	// 描述
	Description    string
	// 指令函数
	Function       func([]io.Reader, []io.Writer, *pb.InstructionStat) error
	// 指令
	Instruction    instruction.Instruction

	Tasks          []*Task
	NetworkType    NetworkType
	IsOnDriverSide bool
	IsPipe         bool
	IsGoCode       bool
	Script         script.Script
	Command        *script.Command // used in Pipe()
	Meta           *StepMetadata
	Params         map[string]interface{}
	RunLocked
}

type Task struct {
	Id           int
	Step         *Step
	InputShards  []*DatasetShard
	InputChs     []*util.Piper // task specific input chans. InputShard may have multiple reading tasks
	OutputShards []*DatasetShard
	Stat         *pb.InstructionStat
}

type RunLocked struct {
	sync.Mutex
	StartTime time.Time
}
