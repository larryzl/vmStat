/*
@Time       : 2019/12/25 12:04 下午
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
	"strconv"
	"sync"
	"time"
)

var wgc sync.WaitGroup
var wgl sync.WaitGroup

func randStr(n int) string {
	rand.Seed(time.Now().UnixNano())
	letter := []byte("abcdefghigklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var str string
	for i := 0; i < n; i++ {
		str += string(letter[rand.Intn(len(letter))])
	}
	return str
}

// 写入文件
func writeData(filePath string, data []byte) error {
	
	fileObj, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = fileObj.Close() }()
	writeObj := bufio.NewWriter(fileObj)
	_, err = writeObj.Write(data)
	if err != nil {
		return err
	}
	err = writeObj.Flush()
	if err != nil {
		return err
	}
	fmt.Println("写入文件完成")
	return nil
}

func generateStrData(n int, data *[]byte, dataChan chan []byte) error {
	fmt.Println("基础数据生成完成")
	threadNum := 2
	for i := 0; i < threadNum; i++ {
		wgc.Add(1)
		fmt.Println("准备进入goroutine ，", i)
		go dataCreate(n/threadNum, dataChan)
		
	}
	wgl.Add(1)
	go dataLoad(data, dataChan)
	wgc.Wait()
	close(dataChan)
	wgl.Wait()
	fmt.Println("生成数据完成")
	return nil
	
}

func dataLoad(data *[]byte, dataChan <-chan []byte) {
	defer wgl.Done()
	for d := range dataChan {
		*data = append(*data, d...)
	}
}

func dataCreate(n int, dataChan chan<- []byte) {
	rand.Seed(time.Now().UnixNano())
	defer wgc.Done()
	pathList := [...]string{"www", "shop", "game", "others", "index", "shop", "news", "game2", "game3", "game4"}
	uuid := make([]string, n)
	uid := make([]string, n)
	for i := 0; i < len(uid); i++ {
		uuid[i] = randStr(10)
		uid[i] = randStr(5)
	}
	for i := 0; i < n; i++ {
		ip := fmt.Sprintf("%d.%d.%d.%d", rand.Intn(100), rand.Intn(100), rand.Intn(100), rand.Intn(10))
		str := fmt.Sprintf("{\"ip\":\"%s\",\"uid\":\"%s\",\"appid\":\"%d\",\"credit\":\"%d\",\"path\":\"%s\",\"uuid\":\"%s\"}\n",
			ip, uid[rand.Intn(n)], rand.Intn(20), rand.Intn(900)+100, pathList[rand.Intn(4)], (uuid)[rand.Intn(n)])
		dataChan <- []byte(str)
	}
	
}

func main() {
	data := make([]byte, 0, 1024)
	dataChan := make(chan []byte, 100)
	
	if len(os.Args) != 2 {
		fmt.Printf("Args Error\nUsage ./generateLog int\n")
		return
	}
	lineNum := os.Args[1]
	n, err := strconv.Atoi(lineNum)
	if err != nil {
		fmt.Printf("Args Error\nUsage ./generateLog int\n")
		return
	}
	fmt.Println("开始生成数据...")
	err = generateStrData(n, &data, dataChan)
	if err != nil {
		fmt.Println(err)
		return
	}
	dateIndex := time.Now().Format("200601021504")
	filePath := path.Join("./", fmt.Sprintf("stats.access.%s.log", dateIndex))
	err = writeData(filePath, data)
	if err != nil {
		fmt.Println(err)
	}
}
