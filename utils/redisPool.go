/*
@Time       : 2020/1/9 3:07 下午
@Author     : lei
@File       : redisConn
@Software   : GoLand
@Desc       :
*/
package utils

import "github.com/gomodule/redigo/redis"

var (
	DataPool *redis.Pool	// 每日数据，临时数据
	QueuePool *redis.Pool	// 常驻数据，队列数据
)


//var pool *redis.DataPool //创建redis连接池

func init() {
	redisHost := "192.168.8.23:8379"
	DataPool = &redis.Pool{ //实例化一个连接池
		MaxIdle: 32, //最初的连接数量
		// MaxActive:1000000,    //最大连接数量
		MaxActive:   1000,   //连接池最大连接数量,不确定可以用0（0表示自动定义），按需分配
		IdleTimeout: 300, //连接关闭时间 300秒 （300秒不使用自动关闭）
		Wait: true,
		Dial: func() (redis.Conn, error) { //要连接的redis数据库
			c, err := redis.Dial("tcp", redisHost,redis.DialDatabase(0))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	QueuePool = &redis.Pool{ //实例化一个连接池
		MaxIdle: 32, //最初的连接数量
		// MaxActive:1000000,    //最大连接数量
		MaxActive:   1000,   //连接池最大连接数量,不确定可以用0（0表示自动定义），按需分配
		IdleTimeout: 300, //连接关闭时间 300秒 （300秒不使用自动关闭）
		Wait: true,
		Dial: func() (redis.Conn, error) { //要连接的redis数据库
			c, err := redis.Dial("tcp", redisHost,redis.DialDatabase(1))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
}

