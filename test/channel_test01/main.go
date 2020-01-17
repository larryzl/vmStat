/*
@Time       : 2019/12/30 10:27 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import "fmt"

/*
开启一个writeData,向管intChan道写入50个整数
开一个readData，从管道intChan读writeData写入的数据
 */



func main() {
	var intChan chan int
	intChan = make(chan int ,10)
	exChan := make(chan bool,1)
	go writeData(intChan)
	go readData(intChan,exChan)
	for{
		_,ok := <- exChan
		if !ok{
			break
		}
	}
	fmt.Println("程序结束")
}

func readData(intChan <-chan int, exChan chan<- bool) {
	for{
		v,ok := <- intChan
		if !ok{
			break
		}
		fmt.Println(v)
	}
	exChan <- true
	close(exChan)
	
}

func writeData(intChan chan <- int) {
	for i:=0;i<50;i++{
		intChan <- i
	}
	close(intChan)
}