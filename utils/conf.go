/*
@Time       : 2020/2/13 10:55 上午
@Author     : lei
@File       : conf
@Software   : GoLand
@Desc       :
*/
package utils

import (
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"os"
)

//const ConfigFile string = "./config.yaml"

var Setting = Config{}

var (
	h    bool
	v    bool
	r    bool
	stop bool
	c    string
	f    string
)
const PID  = "/var/run/vmStat.pid"

type Redis struct {
	IP   string `yaml:"ip"`
	Port string `yaml:"port"`
	Db   int    `yaml:"db"`
}
type Mysql struct {
	IP       string `yaml:"ip"`
	Port     string `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}
type Log struct {
	Path string `yaml:"path"`
}
type Static struct {
	Path string `yaml:"path"`
}

type Config struct {
	Data struct {
		Redis
	}
	Queue struct {
		Redis
	}
	Log
	Mysql
	Static

	Mode    string
	Logfile string
	IpFile  string `yaml:"ipFile"`
}

func init() {
	flag.BoolVar(&h, "h", false, "this help")

	flag.BoolVar(&v, "v", false, "show version and exit")
	flag.BoolVar(&r, "r", false, "calculate 7-day retention rate")
	flag.BoolVar(&stop, "stop", false, "stop vmStat process")

	// 注意 `signal`。默认是 -s string，有了 `signal` 之后，变为 -s signal
	flag.StringVar(&c, "c", "config.yaml", "set configuration `file`")
	flag.StringVar(&f, "f", "", "set log `file` , calculate once and exit ")

	// 改变默认的 Usage，flag包中的Usage 其实是一个函数类型。这里是覆盖默认函数实现，具体见后面Usage部分的分析
	flag.Usage = usage
	flag.Parse()
	if h {
		flag.Usage()
		os.Exit(0)
	} else if v {
		fmt.Println("vmStat version: vmStat/0.1.0")
		os.Exit(0)

	} else {
		data, _ := ReadFilesLines(c)
		// 加载配置文件
		err := yaml.Unmarshal(data, &Setting)
		logger.Debug("加载配置文件完成,位置:%s\n", c)
		if err != nil {
			logger.Error("解析配置文件错误 error: %v", err)
			os.Exit(1)
		}
		if r {
			Setting.Mode = "retention"
		} else if f != "" {
			Setting.Mode = "once"
			Setting.Logfile = f
		} else {
			Setting.Mode = "normal"
		}
	}

}
func usage() {
	fmt.Fprintf(os.Stderr, `vmStat version: vmStat/0.1.0
Usage: vmStat [-hv] [-c filename]

Options:
`)
	flag.PrintDefaults()
}
