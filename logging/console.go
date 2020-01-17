/*
@Time       : 2019/12/19 5:41 下午
@Author     : lei
@File       : console
@Software   : GoLand
@Desc       : 输出日志内容到终端
*/
package logging

import (
	"fmt"
	"strings"
	"time"
)

// ConsoleLog 将日志打印到标准输出
type ConsoleLog struct {
	level Level // 日志输出级别
}

func NewConsoleLogger(level string) *ConsoleLog {
	lv, err := transLevel(level)
	if err != nil {
		panic(err)
	}
	return &ConsoleLog{level: *lv}
}

func (c *ConsoleLog) isPrint(lv Level, format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	if lv >= c.level {
		lvName := strings.ToUpper(levelStr[int(lv)])
		data := fmt.Sprintf("[%s] [%s] %s", time.Now().Format("2006-01-02 15:04:05"), lvName, msg)
		fmt.Print(data)
	}
}

func (c *ConsoleLog) Debug(format string, a ...interface{})   { c.isPrint(DEBUG, format, a...) }
func (c *ConsoleLog) Info(format string, a ...interface{})    { c.isPrint(INFO, format, a...) }
func (c *ConsoleLog) Warning(format string, a ...interface{}) { c.isPrint(WARNING, format, a...) }
func (c *ConsoleLog) Error(format string, a ...interface{})   { c.isPrint(ERROR, format, a...) }
