/*
@Time       : 2020/1/9 3:07 下午
@Author     : lei
@File       : redisConn
@Software   : GoLand
@Desc       :
*/
package utils

import "github.com/gomodule/redigo/redis"

var Pool *redis.Pool

//var pool *redis.Pool //创建redis连接池

func init() {
	redisHost := "192.168.8.23:8379"
	redisDb := 0
	Pool = &redis.Pool{ //实例化一个连接池
		MaxIdle: 16, //最初的连接数量
		// MaxActive:1000000,    //最大连接数量
		MaxActive:   0,   //连接池最大连接数量,不确定可以用0（0表示自动定义），按需分配
		IdleTimeout: 300, //连接关闭时间 300秒 （300秒不使用自动关闭）
		Dial: func() (redis.Conn, error) { //要连接的redis数据库
			c, err := redis.Dial("tcp", redisHost)
			if err != nil {
				return nil, err
			}
			c.Do("SELECT", redisDb)
			return c, nil
		},
	}
}

