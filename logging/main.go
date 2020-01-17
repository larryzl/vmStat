/*
@Time       : 2019/12/19 5:39 下午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package logging

type Logger interface {
	Debug(format string,a ...interface{})
	Info(format string,a ...interface{})
	Warning(format string,a ...interface{})
	Error(format string,a ...interface{})
}
