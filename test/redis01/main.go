package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

var pool *redis.Pool //创建redis连接池

func init() {
	pool = &redis.Pool{ //实例化一个连接池
		MaxIdle: 16, //最初的连接数量
		// MaxActive:1000000,    //最大连接数量
		MaxActive:   0,   //连接池最大连接数量,不确定可以用0（0表示自动定义），按需分配
		IdleTimeout: 300, //连接关闭时间 300秒 （300秒不使用自动关闭）
		Dial: func() (redis.Conn, error) { //要连接的redis数据库
			return redis.Dial("tcp", "192.168.8.23:8379")
		},
	}
}

var wg sync.WaitGroup

func run() {
	c := pool.Get() //从连接池，取一个链接
	defer c.Close() //函数运行结束 ，把连接放回连接池
	defer wg.Done()
	for {
		
		r, err := redis.Strings(c.Do("MGet", "a", "b"))
		if err != nil {
			fmt.Println("get abc faild :", err)
			return
		}
		fmt.Println(r)
		time.Sleep(time.Second)
	}
	pool.Close() //关闭连接池
}

func main() {
	t := time.Now()
	for i := 0; i < 5; i++ {
		
		wg.Add(1)
		go run()
	}
	wg.Wait()
	fmt.Println("运行时间:", time.Now().Sub(t))
}
