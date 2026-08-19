// Package mlog 提供简单的分级日志（Debug/Info/Warn/Error），输出到 stderr。
// 保持极简：全局单例 + 级别过滤，结构化字段通过 key=value 拼进消息。
package mlog

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

type Level int32

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	std    = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	curLvl = int32(LevelInfo)
)

func SetLevel(l Level) { atomic.StoreInt32(&curLvl, int32(l)) }

func level() Level { return Level(atomic.LoadInt32(&curLvl)) }

func logf(l Level, tag, format string, args ...interface{}) {
	if l < level() {
		return
	}
	std.Output(3, fmt.Sprintf("[%s] %s", tag, fmt.Sprintf(format, args...)))
}

func Debug(format string, args ...interface{}) { logf(LevelDebug, "DEBUG", format, args...) }
func Info(format string, args ...interface{})  { logf(LevelInfo, "INFO", format, args...) }
func Warn(format string, args ...interface{})  { logf(LevelWarn, "WARN", format, args...) }
func Error(format string, args ...interface{}) { logf(LevelError, "ERROR", format, args...) }
