/*
@Time       : 2019/12/20 12:05 下午
@Author     : lei
@File       : file
@Software   : GoLand
@Desc       :
*/
package logging

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"reflect"
	"strings"
	"time"
)

type FileLog struct {
	level    Level
	fileName string
	filePath string
	maxSize  int64
	fileObj  *os.File
	duration *time.Duration
}

// NewFileLog 构造方法，如果按时间切割日志，则忽略大小限制
func NewFileLog(level, fileName, filePath string, maxSize int64, duration interface{}) *FileLog {
	lv, err := transLevel(level)
	if err != nil {
		panic(err)
	}
	// 打开文件对象
	fileObj, err := initFile(fileName, filePath)
	if err != nil {
		panic(err)
	}
	
	dValue := reflect.ValueOf(duration)
	
	var d time.Duration
	switch dValue.Kind() {
	case reflect.Int64:
		d = time.Duration(dValue.Int())
	default:
		d = time.Duration(0)
	}
	
	return &FileLog{
		level:    *lv,
		fileName: fileName,
		filePath: filePath,
		maxSize:  maxSize,
		fileObj:  fileObj,
		duration: &d,
	}
}

func initFile(fileName, filePath string) (*os.File, error) {
	fileFullPath := path.Join(filePath, fileName)
	fileObj, err := os.OpenFile(fileFullPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		return nil, fmt.Errorf("打开文件失败:%v", err)
	}
	return fileObj, nil
}

func (f *FileLog) fileBackup() error {
	//	关闭文件-重命名-重新打开文件
	// 关闭文件
	_ = f.fileObj.Close()
	//
	fileFullPath := path.Join(f.filePath, f.fileName)
	bakFileName := fmt.Sprintf("%s_%s.log", strings.TrimRight(fileFullPath, ".log"), time.Now().Format("20060102150405"))
	err := os.Rename(fileFullPath, bakFileName)
	if err != nil {
		return fmt.Errorf("重命名文件失败:%v", err)
	}
	f.fileObj, err = initFile(f.fileName, f.filePath)
	if err != nil {
		return err
	}
	return nil
}

func (f *FileLog) isPrint(lv Level, format string, a ...interface{}) error {
	// 过滤掉不满足日志级别
	if f.level > lv {
		return nil
	}
	/*
		1. 不切割日志，maxSize =0 duration =0
		2. 按大小切割 maxSize > 0 duration =0
		3. 按时间切割 maxSize => 0 duration > 0
	*/
	fileFullPath := path.Join(f.filePath, f.fileName)
	fileStat, err := os.Stat(fileFullPath)
	if err != nil {
		return fmt.Errorf("获取文件状态失败,err:%v", err)
	}
	
	if *f.duration >= time.Minute {
		Day := time.Hour * 24
		switch *f.duration {
		case Day:
			if fileStat.ModTime().Format("20060102") != time.Now().Format("20060102") {
				err = f.fileBackup()
				if err != nil {
					return err
				}
			}
		case time.Hour:
			if fileStat.ModTime().Format("2006010215") != time.Now().Format("2006010215") {
				err = f.fileBackup()
				if err != nil {
					return err
				}
			}
		case time.Minute:
			if fileStat.ModTime().Format("200601021504") != time.Now().Format("200601021504") {
				err = f.fileBackup()
				if err != nil {
					return err
				}
			}
		default:
			panic("日志切割时间格式错误,支持 分钟/小时/天 time.Minute/time.Hour/time.Hour*24")
		}
		
	} else if f.maxSize > 0 {
		//	按大小切割
		// 1 获取文件大小
		// 2 如果大于maxSize， 则按时间重命名
		// 3 重新打开文件写入
		if fileStat.Size() > f.maxSize {
			//重命名文件
			err = f.fileBackup()
			if err != nil {
				return err
			}
		}
	}
	
	//开始写日志
	msg := fmt.Sprintf(format, a...)
	lvName := strings.ToUpper(levelStr[int(lv)])
	data := fmt.Sprintf("[%s] [%s] %s", time.Now().Format("2006-01-02 15:04:05"), lvName, msg)
	
	f.writeData(err, data)
	
	return nil
	
}

func (f *FileLog) writeData(err error, data string)  {
	writeObj := bufio.NewWriterSize(f.fileObj, 4096)
	
	if _, err = writeObj.WriteString(data); err != nil {
		panic( fmt.Errorf("写入缓冲失败,%v", err))
	}
	if _, err = writeObj.Write([]byte(data)); err != nil {
		panic( fmt.Errorf("写入文件失败,%v", err))
	}
	if err := writeObj.Flush(); err != nil {
		panic( fmt.Errorf("刷新文件失败,%v", err))
	}
}

func (f *FileLog) Debug(format string, a ...interface{}) {
	err := f.isPrint(DEBUG, format, a...)
	if err != nil {
		panic(err)
	}
}
func (f *FileLog) Info(format string, a ...interface{}) {
	err := f.isPrint(INFO, format, a...)
	if err != nil {
		panic(err)
	}
}
func (f *FileLog) Warning(format string, a ...interface{}) {
	err := f.isPrint(WARNING, format, a...)
	if err != nil {
		panic(err)
	}
}
func (f *FileLog) Error(format string, a ...interface{}) {
	err := f.isPrint(ERROR, format, a...)
	if err != nil {
		panic(err)
	}
}
