/*
@Time       : 2019/12/24 11:32 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"os"
	"time"
	"vmStat/computed"
	"vmStat/logging"
)
var logger logging.Logger

func main() {
	logger = logging.NewConsoleLogger("debug")
	logger.Debug(">> 开始运行计算程序\n")
	s := time.Now()
	
	filePath := os.Args[1]
	if _,err := os.Stat(filePath);err != nil{
		logger.Debug("读取日志文件出错:%v\n",err)
		return
	}
	c := computed.NewResult(filePath)

	c.Run()
	logger.Debug(">> 程序共运行 %v 秒\n", time.Now().Sub(s))
	
}
