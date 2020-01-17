/*
@Time       : 2019/12/19 5:44 下午
@Author     : lei
@File       : Level
@Software   : GoLand
@Desc       :
*/
package logging

import (
	"errors"
	"strings"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARNING
	ERROR
)

var levelStr = []string{"debug", "info", "warning", "error"}

func transLevel(lv string) (*Level, error) {
	index := -1
	for i, l := range levelStr {
		if strings.ToUpper(lv) == strings.ToUpper(l) {
			index = i
		}
	}
	if index == -1 {
		return nil, errors.New("日志级别错误,请检查")
	} else {
		resLevel := Level(index)
		return &resLevel, nil
	}
}
