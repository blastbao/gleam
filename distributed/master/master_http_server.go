package master

import (
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/chrislusf/gleam/distributed/master/ui"
	"github.com/chrislusf/gleam/pb"
	"github.com/gorilla/mux"
	"github.com/hashicorp/golang-lru"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = 0.01

	var stats []*pb.FlowExecutionStatus
	for _, key := range ms.statusCache.Keys() {
		stat, _ := ms.statusCache.Get(key.(uint32))
		stats = append(stats, stat.(*pb.FlowExecutionStatus))
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Driver.StartTime > stats[j].Driver.StartTime
	})

	args := struct {
		Version   string
		Topology  interface{}
		StartTime time.Time
		Logs      *lru.Cache
		Stats     []*pb.FlowExecutionStatus
	}{
		"0.01",
		ms.Topology,
		ms.startTime,
		ms.statusCache,
		stats,
	}
	ui.MasterStatusTpl.Execute(w, args)
}

func (ms *MasterServer) jobStatusHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	// 提取参数 JobId
	jobId, err := strconv.ParseUint(vars["id"], 10, 32)
	if err != nil {
		log.Printf("Failed to parse job id %s", vars["id"])
		return
	}

	// 查询 JobId 任务状态
	status, ok := ms.statusCache.Get(uint32(jobId))
	if !ok {
		log.Printf("Failed to find job status for %d", jobId)
		return
	}

	// 构造响应
	args := struct {
		Version   string		// 版本号
		Topology  interface{}	// 拓扑
		Status    interface{}	// 状态
		Svg       string		//
		StartTime time.Time		// 开始时间
		Logs      *lru.Cache	// 日志
	}{
		"0.01",
		ms.Topology,
		status.(*pb.FlowExecutionStatus),
		ui.GenSvg(status.(*pb.FlowExecutionStatus)),
		ms.startTime,
		ms.statusCache,
	}

	ui.JobStatusTpl.Execute(w, args)
}
