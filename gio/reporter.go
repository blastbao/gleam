package gio

import (
	"context"
	"sync"
)

func (runner *gleamRunner) report(ctx context.Context, f func() error) error {
	// TODO use context for mapper, reducer, and gleam execute
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	finishedChan := make(chan bool, 1)

	// 启动心跳协程
	var heartbeatWg sync.WaitGroup
	heartbeatWg.Add(1)
	go runner.statusHeartbeat(&heartbeatWg, finishedChan)
	defer heartbeatWg.Wait()

	// 启动协程执行 f() 主逻辑，并在结束时关闭 finishedChan 管道
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		f()
		wg.Done()
	}()
	go func() {
		wg.Wait()
		close(finishedChan)
	}()

	select {
	// 任务结束，上报任务状态；
	case <-finishedChan:
		runner.reportStatus()
	// 超时，报错返回；
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
