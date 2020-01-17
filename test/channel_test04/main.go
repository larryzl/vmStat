/*
@Time       : 2019/12/30 11:14 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

// 存储文件名
var filenameChan chan string
var wg sync.WaitGroup

func randStr(n int) []byte {
	rand.Seed(time.Now().UnixNano())
	resByte := make([]byte, n)
	for i := 0; i < n; i++ {
		resByte[i] = byte(97 + rand.Intn(26))
	}
	return resByte
}


func writeData(filename string, context []byte) error {
	filename = path.Join("./file/",filename)
	fileObj, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("打开文件错误:%v", err)
		return err
	}
	writeObj := bufio.NewWriter(fileObj)
	_, err = writeObj.Write(context)
	if err != nil {
		fmt.Println("写入缓存失败,", err)
		return err
	}
	err = writeObj.Flush()
	if err != nil {
		fmt.Println("刷新文件失败,", err)
		return err
	}
	return nil
}

func writeDataToFile(fChan chan string) {
	for {
		fn, ok := <-filenameChan
		if !ok {
			break
		}
		err := writeData(fn, randStr(1000))
		if err != nil{
			fChan <- fn
		}
	}
	if len(fChan) == 10{
		close(fChan)
	}
}



func main() {
	filenameChan = make(chan string, 10)
	writeFileChan := make(chan string,10)
	go func() {
		for i := 0; i < 10; i++ {
			filename := string(randStr(6)) + ".txt"
			filenameChan <- filename
		}
		close(filenameChan)
	}()
	for i:=0;i<10;i++{
		writeDataToFile(writeFileChan)
	}
	
	
	
	
}
