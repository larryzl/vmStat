/*
@Time       : 2019/12/25 1:59 下午
@Author     : lei
@File       : fileUtils
@Software   : GoLand
@Desc       :
*/
package utils

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"vmStat/logging"
)

type AccessLog struct {
	Ip     string `json:"ip"`
	Uid    string `json:"uid"`
	Appid  string `json:"appid"`
	Credit string `json:"credit"`
	Path   string `json:"path"`
	Uuid   string `json:"uuid"`
	Line 	uint64
	Country string
	Province string
	City string
}

var (
	logger       = logging.NewConsoleLogger("debug")
)

// ReadFilesLines 按照行读取文件
func ReadFilesLines(filePath string) (data []byte, err error) {
	fileObj, err := os.Open(filePath)
	if err != nil {
		fmt.Println("打开文件失败")
		return nil, err
	}
	defer fileObj.Close()
	buf := make([]byte, 16384)
	readObj := bufio.NewReaderSize(fileObj,16384)
	//data = make([]byte, 0, 4096)
	for {
		n ,err := readObj.Read(buf)
		//buf, err := readObj.ReadBytes('\n')
		if err != nil && err != io.EOF {
			fmt.Println("读文件出错")
			return nil, err
		}
		if n == 0{
			break
		}
		data = append(data, buf[:n]...)
	}
	return data, nil
}

func WriteFiles(filePath string, d []byte) (err error) {
	fileObj, err := os.OpenFile(filePath,os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644)
	if err != nil{
		logger.Error("打开文件错误:%v",err)
		return
	}
	defer fileObj.Close()
	writeObj := bufio.NewWriter(fileObj)
	_,err = writeObj.Write(d)
	if err != nil{
		logger.Error("写入缓存错误:%v",err)
		return
	}
	err = writeObj.Flush()
	if err != nil{
		logger.Error("写入文件错误:%v",err)
		return
	}
	return nil
}


func SerData(filePath string) (accessData []*AccessLog, err error) {
	data, err := ReadFilesLines(filePath)
	if err != nil {
		return nil, err
	}
	//var mapResult []map[string]interface{}
	strData := strings.ReplaceAll(string(data), "\n", ",")
	strData = "[" + strData
	strData = strings.TrimRight(strData, ",")
	strData += "]"
	err = json.Unmarshal([]byte(strData), &accessData)
	if err != nil {
		fmt.Println("解析json出错")
		return nil, err
	}
	return
}
