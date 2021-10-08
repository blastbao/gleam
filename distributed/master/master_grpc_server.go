package master

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"context"
	"github.com/chrislusf/gleam/pb"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/golang-lru"
)

type MasterServer struct {
	Topology     *Topology
	statusCache  *lru.Cache
	logDirectory string
	startTime    time.Time
}

func newMasterServer(logDirectory string) *MasterServer {
	m := &MasterServer{
		Topology:     NewTopology(),
		logDirectory: logDirectory,
		startTime:    time.Now(),
	}
	m.statusCache, _ = lru.NewWithEvict(512, m.onCacheEvict)
	if strings.HasSuffix(m.logDirectory, "/") {
		m.logDirectory = strings.TrimSuffix(m.logDirectory, "/")
	}
	m.onStartup()
	return m
}

func (ms *MasterServer) GetResources(ctx context.Context, in *pb.ComputeRequest) (*pb.AllocationResult, error) {
	var err error
	dcName := in.GetDataCenter()
	if dcName == "" {
		dcName, err = ms.Topology.allocateDataCenter(in.GetComputeResources())
		if err != nil {
			return nil, err
		}
	}
	dc, hasDc := ms.Topology.GetDataCenter(dcName)
	if !hasDc {
		return nil, fmt.Errorf("Failed to find existing data center: %s", dcName)
	}

	allocations := ms.Topology.findServers(dc, in.GetComputeResources())

	log.Printf("%v requests %+v, allocated %+v", in.FlowHashCode, in.GetComputeResources(), allocations)

	return &pb.AllocationResult{
		Allocations: allocations,
	}, nil

}

func (ms *MasterServer) SendHeartbeat(stream pb.GleamMaster_SendHeartbeatServer) error {
	var location *pb.Location
	for {
		heartbeat, err := stream.Recv()
		if err == nil {
			if location == nil {
				location = heartbeat.Location
				log.Printf("added agent: %v", location)
			}
		} else {
			if location != nil {
				ms.Topology.deleteAgentInformation(location)
			}
			log.Printf("lost agent: %v", location)

			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
		ms.Topology.UpdateAgentInformation(heartbeat)
	}
}

func (ms *MasterServer) SendFlowExecutionStatus(stream pb.GleamMaster_SendFlowExecutionStatusServer) (err error) {
	var id uint32
	defer func() {
		if id == 0 {
			return
		}
		status, ok := ms.statusCache.Get(id)
		if !ok {
			return
		}
		fes := status.(*pb.FlowExecutionStatus)
		if err != nil && err != io.EOF {
			fes.Error = err.Error()
		}
		if fes.Driver.StopTime == 0 {
			fes.Driver.StopTime = time.Now().UnixNano()
		}
		ms.statusCache.Add(id, fes)

		data, _ := proto.Marshal(fes)
		ioutil.WriteFile(fmt.Sprintf("%s/f%d.log", ms.logDirectory, id), data, 0644)
	}()

	for {
		status, err := stream.Recv()

		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		id = status.GetId()
		ms.statusCache.Add(id, status)
	}
}

func (ms *MasterServer) onStartup() {
	files, _ := filepath.Glob(fmt.Sprintf("%s/f[0-9]*\\.log", ms.logDirectory))
	for _, f := range files {
		data, _ := ioutil.ReadFile(f)
		status := &pb.FlowExecutionStatus{}
		if err := proto.Unmarshal(data, status); err == nil {
			// println("loading", f, "for", status.GetId())
			ms.statusCache.Add(status.GetId(), status)
		} else {
			os.Remove(f)
		}
	}
}

func (ms *MasterServer) onCacheEvict(key interface{}, value interface{}) {
	id := key.(uint32)
	os.Remove(fmt.Sprintf("%s/f%d.log", ms.logDirectory, id))
}
