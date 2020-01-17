/*
@Time       : 2019/12/30 12:17 下午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

type Num struct {
	age int64
}

type People struct {
	Name string
	*Num
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

func NewPeople() *People {
	return &People{}
	
}

func mapTest(m map[string][]string) {
	for i:=0;i<10;i++{
		j := i*2
		m["a"] = append(m["a"],strconv.Itoa(j))
	}
}

func sliceTest(s []string)  {
	for i:=0;i<10;i++{
		j:= i*2
		s = append(s, strconv.Itoa(j))
	}
	fmt.Println("in func:" ,s)
}

var ap =make(map[string]*People,10)

func main()  {
	
	for i:=0;i<10;i++{
		name:= randStr(4)
		ap[name] = &People{
			Name: name,
			Num: &Num{age:0,},
		}
		ap[name].age++
	}
	for i,v := range ap{
		fmt.Println(i,v.age)
	}
	
	m1 := make(map[string][]string,10)
	m1["a"] = make([]string,10)
	for i:=0;i<10;i++{
		m1["a"][i] = strconv.Itoa(i)
	}
	fmt.Println("main:before:",m1)
	fmt.Println("-------------")
	mapTest(m1)
	fmt.Println("main:after:",m1)
	fmt.Println("-------------++++++++++++++")
	
	s1 := make([]string,10)
	for i:=0;i<10;i++{
		s1[i] = strconv.Itoa(i)
	}
	fmt.Println("main:before:",s1)
	sliceTest(s1)
	fmt.Println("-------------")
	fmt.Println("main:after:",s1)
}