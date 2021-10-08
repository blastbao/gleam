package util

import (
	"os"
	"runtime"
	"strings"
)

func UserHomeDir() string {
	// 检查操作系统
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOMEDRIVE") + os.Getenv("HOMEPATH")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		return home
	}
	return os.Getenv("HOME")
}

func CleanPath(path string) string {
	tiltIndex := strings.Index(path, "~")
	if tiltIndex >= 0 {
		path = path[:tiltIndex] + UserHomeDir() + path[tiltIndex+1:]
	}
	return path
}
