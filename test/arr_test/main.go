/*
@Time       : 2020/1/17 11:41 上午
@Author     : lei
@File       : arr_test
@Software   : GoLand
@Desc       :
*/
package arr_test

import (
	"bytes"
	"math/rand"
	"testing"
	"time"
)

func randomArr(l int) (arr []string) {
	rand.Seed(time.Now().UnixNano())
	str := []byte("abcdefghigklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	for i := 0; i < l; i++ {

		length := rand.Intn(4)
		a := make([]byte, length)
		for j := 0; j < length; j++ {
			a[j] = str[rand.Intn(len(str))]
		}
		arr = append(arr, string(a))
	}
	return
}

func InArr() bool {
	var a = randomArr(10000)
	s := a[rand.Intn(len(a))]
	for _, v := range a {
		if s == v {
			return true
		}
	}
	return false
}

func InMap() bool {
	var a = randomArr(10000)
	m := make(map[string]int, len(a))
	s := a[rand.Intn(len(a))]
	for i, v := range a {
		m[v] = i
	}
	if _, ok := m[s]; ok {
		return true
	}
	return false

}

func TestInArr(t *testing.T) {
	r := InArr()
	if r == false {
		t.Errorf("InArr(a) = %v; want true", r)
	}
}

func BenchmarkInArr(b *testing.B) {
	r := InArr()
	if r == false {
		b.Errorf("InArr(a) = %v; want true", r)
	}
}
func BenchmarkInMap(b *testing.B) {
	r := InMap()
	if r == false {
		b.Errorf("InArr(a) = %v; want true", r)
	}
}



func BenchmarkTemplateParallel(b *testing.B) {

	b.RunParallel(func(pb *testing.PB) {
		var buf bytes.Buffer
		for pb.Next() {
			buf.Reset()
			InArr()
		}
	})
}
