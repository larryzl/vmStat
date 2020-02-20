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

	//"os"
	"time"
	"vmStat/computed"
	"vmStat/logging"
	"vmStat/utils"
)

var logger logging.Logger

func main() {
	logger = logging.NewConsoleLogger("debug")
	//
	//filePath := os.Args[1]
	//if _,err := os.Stat(filePath);err != nil{
	//	logger.Debug("读取日志文件出错:%v\n",err)
	//	return
	//}
	//

	switch utils.Setting.Mode {
	case "retention":
		logger.Debug("开始计算留存率\n")
		c := computed.New()
		c.Retention()
	case "once":
		logger.Debug("开始执行测试模式\n")
		s := time.Now()
		c := computed.New()
		err := c.Run(utils.Setting.Logfile)
		if err != nil {
			logger.Error("err:%v\n", err)
			os.Exit(1)
		}
		logger.Debug(">> 程序共运行 %v 秒\n", time.Now().Sub(s))
		os.Exit(0)
	case "normal":
		logger.Debug("开始执行生产模式\n")
		for {
			s := time.Now()
			c := computed.New()
			err := c.Run("")
			if err == nil {
				logger.Debug(">> 程序共运行 %v 秒\n", time.Now().Sub(s))
			} else {
				time.Sleep(10 * time.Second)
			}
		}
	}

}
