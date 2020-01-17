/*
@Time       : 2019/12/30 10:35 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
)

//var wg sync.WaitGroup


func main() {
	
	numChan := make(chan int,20)
	resChan := make(chan int,2000)
	exChan := make(chan bool,8)
	go createData(numChan)
	for i:=0;i<8;i++{
		go popData(numChan,resChan,exChan)
	}
	n :=1
	for v := range resChan{
		fmt.Printf("res[%d]=%d\n",n,v)
		n++
	}
}

func popData(numChan <-chan int, resChan chan<- int, exChan chan<- bool) {
	for {
		num,ok:= <- numChan
		if !ok {break}
		res := 0
		for i:=0;i<=num;i++{
			res += i
		}
		resChan<- res
	}
	exChan <- true
	if len(exChan) == 8{
		close(resChan)
	}
	fmt.Println(len(exChan))
}

func createData(numChan chan int) {
	for i:=1;i<=2000;i++{
		numChan <- i
	}
	close(numChan)
}