package agent

import (
	"io"
	"log"

	"github.com/chrislusf/gleam/util"
)

func (as *AgentServer) handleLocalWriteConnection(reader io.Reader, writerName, channelName string, readerCount int) {
	// 创建 channelName 数据集
	dsStore := as.storageBackend.CreateNamedDatasetShard(channelName)

	log.Printf("on disk %s starts writing %s expected reader:%d", writerName, channelName, readerCount)


	/// 下面不断从 reader 中读取数据，并写入到 channelName 数据集上

	var count int64
	messageWriter := util.NewBufferedMessageWriter(dsStore, util.BUFFER_SIZE)

	for {
		message, err := util.ReadMessage(reader)
		if err == io.EOF {
			// println("agent recv eof:", string(message.Bytes()))
			break
		}
		if err == nil {
			count += int64(len(message))
			messageWriter.WriteMessage(message)
			// println("agent recv:", string(message.Bytes()))
		} else {
			log.Printf("on disk %s Failed to write to %s: %v", writerName, channelName, err)
		}
	}

	messageWriter.Flush()
	util.WriteEOFMessage(dsStore)

	log.Printf("on disk %s finished writing %s %d bytes", writerName, channelName, count)

}
