/*
@Time       : 2019/12/30 11:06 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"math/rand"
	"time"
)

type Person struct {
	Name    string
	Age     int
	Address string
}

func randStr(l int) string {
	rand.Seed(time.Now().UnixNano())
	name := make([]byte, l)
	for i := 0; i < l; i++ {
		name[i] = byte(97 + rand.Intn(26))
	}
	return string(name)
}

func generateData(pChan chan<- Person) {
	
	for i := 0; i < 10; i++ {
		p := Person{
			Name:    randStr(4),
			Age:     rand.Intn(30),
			Address: randStr(15),
		}
		pChan <- p
	}
	close(pChan)
}

func main() {
	pChan := make(chan Person, 10)
	generateData(pChan)
	for i:= range pChan{
		fmt.Println(i)
	}
}
