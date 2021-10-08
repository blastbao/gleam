package util

import (
	"context"
)

func ExecuteWithCleanup(parentContext context.Context, onExecute func() error, onCleanup func()) error {
	ctx, cancel := context.WithCancel(parentContext)

	errChan := make(chan error)

	// 启动协程执行 onExecute()
	go func() {
		errChan <- onExecute()
	}()

	// 等待 onExecute() 执行完毕或者超时
	select {
	case err := <-errChan:
		cancel()
		return err
	case <-ctx.Done():
		onCleanup()
		return ctx.Err()
	}

}
