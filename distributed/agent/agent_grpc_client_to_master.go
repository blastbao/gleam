package agent

import (
	"fmt"
	"log"
	"time"

	"context"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
)


// 定时发送心跳给 Master
func (as *AgentServer) heartbeat() {
	for {
		err := as.doHeartbeat(10 * time.Second)
		if err != nil {
			time.Sleep(30 * time.Second)
		}
	}
}

func (as *AgentServer) doHeartbeat(sleepInterval time.Duration) error {
	// 获取 Conn
	grpcConnection, err := util.GleamGrpcDial(as.Master, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	// 获取 Master Client
	client := pb.NewGleamMasterClient(grpcConnection)

	// 获取 Heartbeat Stream
	stream, err := client.SendHeartbeat(context.Background())
	if err != nil {
		log.Printf("SendHeartbeat error: %v", err)
		return err
	}

	// 发送 Heartbeat 包给 Master
	as.sendOneHeartbeat(stream)

	log.Printf("Heartbeat to %s", as.Master)

	ticker := time.NewTicker(sleepInterval)
	for {
		select {
		// 如果分配资源发生变化，就通过 Heartbeat 通知 Master
		case <-as.allocatedHasChanges:
			if err := as.sendOneHeartbeat(stream); err != nil {
				return err
			}
		// 定时上报 Heartbeat 给 Master
		case <-ticker.C:
			if err := as.sendOneHeartbeat(stream); err != nil {
				return err
			}
		}
	}

}

func (as *AgentServer) sendOneHeartbeat(stream pb.GleamMaster_SendHeartbeatClient) error {
	as.allocatedResourceLock.Lock()

	// 构造心跳包
	heartbeat := &pb.Heartbeat{
		// 位置信息
		Location: &pb.Location{
			DataCenter: *as.Option.DataCenter,	// 数据中心
			Rack:       *as.Option.Rack,		// 机架
			Server:     *as.Option.Host,		// 主机地址
			Port:       int32(*as.Option.Port),	// 端口号
		},
		// 计算资源
		Resource:  as.computeResource,
		// 已分配资源
		Allocated: proto.Clone(as.allocatedResource).(*pb.ComputeResource),
	}
	as.allocatedResourceLock.Unlock()

	// log.Printf("Reporting allocated %v", as.allocatedResource)

	// 发送心跳包
	if err := stream.Send(heartbeat); err != nil {
		log.Printf("%v.Send(%v) = %v", stream, heartbeat, err)
		return err
	}

	return nil
}
