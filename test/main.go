/*
@Time       : 2019/12/26 3:19 下午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
	"vmStat/utils"
	"vmStat/utils/ip2region"
)

func funcName() {
	region, err := ip2region.New("../utils/ip2region/ip2region.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer region.Close()
	
	begin := time.Now()
	ip := ip2region.IpInfo{}
	
	ip, err = region.MemorySearch("123.59.110.23")
	fmt.Println(time.Now().Sub(begin))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(ip.Country)
	fmt.Println(ip.Province)
	fmt.Println(ip.City)
}

func randStr(n int) string {
	rand.Seed(time.Now().UnixNano())
	letter := []byte("abcdefghigklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var str string
	for i := 0; i < n; i++ {
		str += string(letter[rand.Intn(len(letter))])
	}
	return str
}

var wg sync.WaitGroup
var redis *utils.RedisClient = utils.NewRedisConn("192.168.8.23:8379", "", 0)
var dataChan chan []interface{}

func main() {
	var s time.Time
	dataCount := 100000
	threads := 50
	flag := "get"
	switch flag {
	case "get":
		s = redisGetThread(dataCount, threads)
	case "set":
		s = redisSetThread(dataCount, threads)
	}
	fmt.Printf("共运行 %v 秒", time.Now().Sub(s))
}

func redisGetThread(dataCount int, threads int) time.Time {
	dataChan = make(chan []interface{},dataCount)
	
	fmt.Println("正在生成数据。。。")
	s2 := time.Now()
	data := make([]string, dataCount)
	for i := 0; i < len(data); i++ {
		data[i] = randStr(4)
	}
	fmt.Println("生成数据完成。。。用时",time.Now().Sub(s2))
	index := 0
	length := len(data) / threads
	s := time.Now()
	fmt.Printf("开始运行...\n查询 %v 条数据，共运行 %v 进程\n", dataCount,threads)
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go redisGet(data[index : index+length],dataChan)
		index += length
	}
	wg.Wait()
	for i:= range dataChan{
		fmt.Println(i)
	}
	return s
}

func redisSetThread(dataCount int, threads int) time.Time {
	fmt.Println("正在生成数据。。。")
	s2 := time.Now()
	data := make([]interface{}, dataCount*2)
	for i := 0; i < len(data); i++ {
		data[i] = randStr(4)
	}
	fmt.Println("生成数据完成。。。用时",time.Now().Sub(s2))
	index := 0
	length := len(data) / threads
	s := time.Now()
	fmt.Printf("开始运行...\n写入 %v 条数据，共运行 %v 进程\n", dataCount,threads)
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go redisSet(data[index : index+length])
		index += length
	}
	wg.Wait()
	return s
}

func redisGet(data []string, c chan<- []interface{}) {
	
	defer wg.Done()
	res,err := redis.MGet(data...)
	if err != nil {
		fmt.Println(err)
	}
	c <- res
	//fmt.Println(res)
}

func redisSet(data []interface{}) {
	defer wg.Done()
	
	_, err := redis.MSet(data...)
	if err != nil {
		fmt.Println(err)
		return
	}
}
