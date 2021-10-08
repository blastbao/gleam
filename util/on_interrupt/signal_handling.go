// +build darwin freebsd netbsd openbsd
// +build !plan9,!windows,!linux

package on_interrupt

import (
	"os"
	"os/signal"
	"syscall"
)

func OnInterrupt(fn func(), onExitFunc func()) {
	// deal with control+c,etc
	signalChan := make(chan os.Signal, 1)
	// controlling terminal close, daemon not exit
	signal.Ignore(syscall.SIGHUP)
	signal.Notify(signalChan,
		os.Interrupt,
		os.Kill,
		syscall.SIGALRM,
		// syscall.SIGHUP,
		syscall.SIGINFO, // this causes windows to fail
		syscall.SIGINT,
		syscall.SIGTERM,
		// syscall.SIGQUIT, // Quit from keyboard, "kill -3"
	)
	go func() {

		for sig := range signalChan {
			fn()
			// 如果是 `SIGINFO` 信号，就啥也不做，否则调用 os.Exit(0) 退出进程。
			if sig == syscall.SIGINFO {
				// do nothing
			} else {
				if onExitFunc != nil {
					onExitFunc()
				}
				os.Exit(0)
			}
		}

	}()
}
