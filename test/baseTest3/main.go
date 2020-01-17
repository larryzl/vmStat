/*
@Time       : 2020/1/10 11:55 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"sync"
)

var wg sync.WaitGroup

func main() {
	num := make([]int, 1000)
	for i := 0; i < 1000; i++ {
		num[i] = i+1
	}
	
	ch1 := make(chan int, 10)
	ch2 := make(chan int, 16)
	
	go func(num []int, ch1 chan<- int) {
		for i := range num {
			ch1 <- i
		}
		close(ch1)
	}(num, ch1)
	
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go filter(ch1, ch2)
	}
	go func() {
		wg.Wait()
		close(ch2)
	}()
	
	for {
		i,ok := <- ch2
		if !ok{
			break
		}
		fmt.Println(i)
	}

	
	
}

func filter(ch1 <-chan int, ch2 chan<- int) {
	defer wg.Done()
	for i := range ch1 {
		if isPrime(i) {
			ch2 <- i
		}
	}
}

func isPrime(num int) bool {
	for i := 2; i < num-1; i++ {
		if num%i == 0 {
			return false
		}
	}
	return true
}
