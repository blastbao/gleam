package flow

import (
	"fmt"
	"io"
	"net"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
)

type Sourcer interface {
	Generate(*Flow) *Dataset
}

// Read accepts a function to read data into the flow, creating a new dataset.
// This allows custom complicated pre-built logic for new data sources.
//
// Read 接受一个函数，将数据读入流，创建一个新的数据集。
// 这允许为新数据源定制复杂的预构建逻辑。
//
//
func (flow *Flow) Read(s Sourcer) (ret *Dataset) {
	return s.Generate(flow)
}

// Listen receives textual inputs via a socket.
// Multiple parameters are separated via tab.
//
//
func (flow *Flow) Listen(network, address string) (ret *Dataset) {
	fn := func(writer io.Writer, stats *pb.InstructionStat) error {
		listener, err := net.Listen(network, address)
		if err != nil {
			return fmt.Errorf("Fail to listen on %s %s: %v", network, address, err)
		}
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("Fail to accept on %s %s: %v", network, address, err)
		}
		defer conn.Close()

		return util.TakeTsv(conn, -1, func(message []string) error {
			stats.InputCounter++
			var row []interface{}
			for _, m := range message {
				row = append(row, m)
			}
			stats.OutputCounter++
			return util.NewRow(util.Now(), row...).WriteTo(writer)
		})

	}
	return flow.Source(address, fn)
}

// Source produces data feeding into the flow.
// Function f writes to this writer.
// The written bytes should be MsgPack encoded []byte.
// Use util.EncodeRow(...) to encode the data before sending to this channel.
//
//
// Source 负责为 flow 创建一个具有 1 个分片的数据集，
//
//
//
//
//
//
func (flow *Flow) Source(name string, f func(io.Writer, *pb.InstructionStat) error) (ds *Dataset) {
	// 为 flow 创建一个具有 1 个分片的数据集 ds 。
	ds = flow.NewNextDataset(1)
	// 为 flow 创建一个类型为 `OneShardToOneShard` 的 step ，其输出数据集为 ds ，
	// 当 flow 被执行时，在 step 执行完成后，ds 便被填充好数据。
	step := flow.AddOneToOneStep(nil, ds)

	// 设置 step 的其它字段
	step.IsOnDriverSide = true
	step.Name = name
	step.Function = func(readers []io.Reader, writers []io.Writer, stats *pb.InstructionStat) error {
		errChan := make(chan error, len(writers))
		// 启动 n 个协程负责写数据到 writers 上。
		for _, writer := range writers {
			go func(writer io.Writer) {
				errChan <- f(writer, stats)
			}(writer)
		}
		// 等待 n 个协程退出。
		for range writers {
			err := <-errChan
			if err != nil {
				return err
			}
		}
		return nil
	}
	return
}

// Channel accepts a channel to feed into the flow.
func (flow *Flow) Channel(ch chan interface{}) (ret *Dataset) {
	ret = flow.NewNextDataset(1)
	step := flow.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Channel"
	step.Function = func(readers []io.Reader, writers []io.Writer, stat *pb.InstructionStat) error {
		for data := range ch {
			stat.InputCounter++
			err := util.NewRow(util.Now(), data).WriteTo(writers[0])
			if err != nil {
				return err
			}
			stat.OutputCounter++
		}
		return nil
	}
	return
}

// Bytes begins a flow with an [][]byte
func (flow *Flow) Bytes(slice [][]byte) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range slice {
			inputChannel <- data
			// println("sent []byte of size:", len(data), string(data))
		}
		close(inputChannel)
	}()

	return flow.Channel(inputChannel)
}

// Strings begins a flow with an []string
func (flow *Flow) Strings(lines []string) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range lines {
			inputChannel <- []byte(data)
		}
		close(inputChannel)
	}()

	return flow.Channel(inputChannel)
}

// Ints begins a flow with an []int
func (flow *Flow) Ints(numbers []int) (ret *Dataset) {
	inputChannel := make(chan interface{})

	go func() {
		for _, data := range numbers {
			inputChannel <- data
		}
		close(inputChannel)
	}()

	return flow.Channel(inputChannel)
}

// Slices begins a flow with an [][]interface{}
func (flow *Flow) Slices(slices [][]interface{}) (ret *Dataset) {

	ret = flow.NewNextDataset(1)
	step := flow.AddOneToOneStep(nil, ret)
	step.IsOnDriverSide = true
	step.Name = "Slices"
	step.Function = func(readers []io.Reader, writers []io.Writer, stat *pb.InstructionStat) error {
		for _, slice := range slices {
			stat.InputCounter++
			err := util.NewRow(util.Now(), slice).WriteTo(writers[0])
			if err != nil {
				return err
			}
			stat.OutputCounter++
		}
		return nil
	}
	return

}
