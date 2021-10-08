// Package agent runs on servers with computing resources, and executes
// tasks sent by driver.
package agent

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
)



type AgentServerOption struct {
	Master       *string
	Host         *string
	Port         *int32
	Dir          *string	// 路径
	DataCenter   *string
	Rack         *string
	MaxExecutor  *int32
	MemoryMB     *int64
	CPULevel     *int32
	CleanRestart *bool
}


type AgentServer struct {
	Option                  *AgentServerOption
	Master                  string
	computeResource         *pb.ComputeResource
	allocatedResource       *pb.ComputeResource
	allocatedHasChanges     chan struct{}
	allocatedResourceLock   sync.Mutex
	storageBackend          *LocalDatasetShardsManager
	inMemoryChannels        *LocalDatasetShardsManagerInMemory
	receiveFileResourceLock sync.Mutex
}



func RunAgentServer(option *AgentServerOption) {

	// 绝对路径
	absoluteDir, err := filepath.Abs(util.CleanPath(*option.Dir))
	if err != nil {
		panic(err)
	}
	println("starting in", absoluteDir)
	option.Dir = &absoluteDir

	//
	as := &AgentServer{
		Option:           option,
		Master:           *option.Master,
		storageBackend:   NewLocalDatasetShardsManager(*option.Dir, int(*option.Port)),
		inMemoryChannels: NewLocalDatasetShardsManagerInMemory(),
		computeResource: &pb.ComputeResource{
			CpuCount: int32(*option.MaxExecutor),
			CpuLevel: int32(*option.CPULevel),
			MemoryMb: *option.MemoryMB,
		},
		allocatedResource:   &pb.ComputeResource{},
		allocatedHasChanges: make(chan struct{}, 5),
	}

	go as.storageBackend.purgeExpiredEntries()
	go as.inMemoryChannels.purgeExpiredEntries()
	go as.heartbeat() // 定时发送心跳给 Master


	// 监听 tcp
	tcpListener, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *option.Host, *option.Port))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("AgentServer tcp starts on", fmt.Sprintf("%v:%d", *option.Host, *option.Port))

	// 监听 tcp
	grpcListener, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *option.Host, *option.Port+10000))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("AgentServer grpc starts on", fmt.Sprintf("%v:%d", *option.Host, *option.Port+10000))

	//
	if *option.CleanRestart {
		if fileInfos, err := ioutil.ReadDir(as.storageBackend.dir); err == nil {
			suffix := fmt.Sprintf("-%d.dat", *option.Port)
			for _, fi := range fileInfos {
				name := fi.Name()
				if !fi.IsDir() && strings.HasSuffix(name, suffix) {
					// println("removing old dat file:", name)
					os.Remove(filepath.Join(as.storageBackend.dir, name))
				}
			}
		}
	}

	// 启动 grpc 服务
	go as.serveGrpc(grpcListener)
	// 启动 tcp 服务
	go as.serveTcp(tcpListener)

	select {}
}

// Run starts the heartbeating to master and starts accepting requests.
func (as *AgentServer) serveTcp(listener net.Listener) {
	for {
		// Listen for an incoming connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			continue
		}

		// Handle connections in a new goroutine.
		go func() {
			defer conn.Close()
			// 设置超时为空
			if err = conn.SetDeadline(time.Time{}); err != nil {
				fmt.Printf("Failed to set timeout: %v\n", err)
			}
			// 设置长链接
			if c, ok := conn.(*net.TCPConn); ok {
				c.SetKeepAlive(true)
			}
			// 处理请求
			as.handleRequest(conn)
		}()
	}
}

func (as *AgentServer) handleRequest(conn net.Conn) {
	// 读取请求
	data, err := util.ReadMessage(conn)
	if err != nil {
		log.Printf("Failed to read command:%v", err)
		return
	}

	// 反序列化请求
	newCmd := &pb.ControlMessage{}
	if err := proto.Unmarshal(data, newCmd); err != nil {
		log.Fatal("unmarshaling error: ", err)
	}

	// 执行请求
	as.handleCommandConnection(conn, newCmd)
}

func (as *AgentServer) handleCommandConnection(conn net.Conn, command *pb.ControlMessage) {

	// 读请求
	if command.GetReadRequest() != nil {
		// 读内存
		if !command.GetIsOnDiskIO() {
			as.handleInMemoryReadConnection(conn, command.ReadRequest.ReaderName, command.ReadRequest.ChannelName)
		// 读磁盘
		} else {
			as.handleReadConnection(conn, command.ReadRequest.ReaderName, command.ReadRequest.ChannelName)
		}
	}

	// 写请求
	if command.GetWriteRequest() != nil {
		// 写内存
		if !command.GetIsOnDiskIO() {
			as.handleLocalInMemoryWriteConnection(conn, command.WriteRequest.WriterName, command.WriteRequest.ChannelName, int(command.GetWriteRequest().GetReaderCount()))
		// 写磁盘
		} else {
			as.handleLocalWriteConnection(conn, command.WriteRequest.WriterName, command.WriteRequest.ChannelName, int(command.GetWriteRequest().GetReaderCount()))
		}
	}
}
