/*
@Time       : 2020/1/2 6:45 下午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"time"
)

type people struct {
	name string
	age  int
}

func timeFormat(s string) (string, error) {
	t, err := time.Parse("2006010215", s)
	if err != nil {
		fmt.Println(err)
		return "",err
	}
	return t.Format("2006-01-02 15:00:00"),nil
}

func main() {
	t,err := timeTest("2019112509")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(t)
}
