package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"vmStat/utils"
)

func Add(a, b int) int {
	return a + b
}

type dataResult struct {
	filePath    string
	datePrefix  string // 文件时间戳
	hourPrefix  string
	dayAppidUid map[string][]interface{}
	timeResult  map[string]*timeField
	//areaResult  map[string]*AreaFields
	//newsResult  []*UserFields
	//:= make(map[string][]interface{},10)
}

func NewComputed(filePath string) *dataResult {
	dataPrefixArray := strings.Split(filePath, ".")
	dataPrefix := dataPrefixArray[len(dataPrefixArray)-2][:8]
	hourPrefix := dataPrefixArray[len(dataPrefixArray)-2][:10]
	dayAppidUid := make(map[string][]interface{}, 10)
	var timeResult = make(map[string]*timeField, 100)
	//var areaResult = make(map[string]*AreaFields, 100)
	return &dataResult{filePath: filePath, datePrefix: dataPrefix, dayAppidUid: dayAppidUid, hourPrefix: hourPrefix, timeResult: timeResult} //, areaResult: areaResult}
}

type AccessLog struct {
	Ip     string `json:"ip"`
	Uid    string `json:"uid"`
	Appid  string `json:"appid"`
	Credit string `json:"credit"`
	Path   string `json:"path"`
	Uuid   string `json:"uuid"`
}

type statisticItem struct {
	pv        int64
	uv        int64
	pathUv    int64
	appUv     int64
	pathAppUv int64
	ip        int64
	appIp     int64
}

type timeField struct {
	appid string
	path  string
	*statisticItem
}

var lock sync.Mutex

func NewTimeField(appid, path string) *timeField {
	s := statisticItem{pv: 0, uv: 0, pathUv: 0, appUv: 0, pathAppUv: 0, ip: 0, appIp: 0}
	return &timeField{
		appid:         appid,
		path:          path,
		statisticItem: &s,
	}
}

var AllAccessLog []*AccessLog
var accessLogChan chan *AccessLog
var wg sync.WaitGroup
var dateStr = "20200108"
var redisClient = utils.NewRedisConn("192.168.8.23:8379", "", 0)

func parasData(filePath string) error {
	data, err := utils.ReadFilesLines(filePath)
	if err != nil {
		return err
	}
	//var mapResult []map[string]interface{}
	strData := "[" + strings.ReplaceAll(string(data), "\n", ",")
	strData = strings.TrimRight(strData, ",")
	strData += "]"
	err = json.Unmarshal([]byte(strData), &AllAccessLog)
	if err != nil {
		fmt.Println("解析json出错")
		return err
	}
	return nil
}

func (d dataResult) Run() {
	err := parasData(d.filePath)
	if err != nil {
		fmt.Println("凡序列化数据错误:", err)
	}
	accessLogChan = make(chan *AccessLog, 50)
	go func(a chan<- *AccessLog) {
		for _, v := range AllAccessLog {
			a <- v
		}
		close(a)
	}(accessLogChan)
	
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go d.Computed(accessLogChan)
	}
	wg.Wait()
	//for i, v := range d.timeResult {
	//	fmt.Println(i, v.uv)
	//}
}

func (d dataResult) Computed(accessLogChan <-chan *AccessLog) {
	defer wg.Done()
	print("开启一个goroutine\n")
	
	uvMap := make(map[string]string, 100)
	pathUvMap := make(map[string]string, 100)
	
	for v := range accessLogChan {
		// 计算UV { key : appid+path }
		lock.Lock()
		if _, ok := d.timeResult[v.Appid+":"+v.Path]; !ok {
			d.timeResult[v.Appid+":"+v.Path] = NewTimeField(v.Appid, v.Path)
		}
		lock.Unlock()
		
		if _, ok := uvMap[v.Uuid]; !ok {
			uvMap[v.Uuid] = v.Appid + ":" + v.Path
		}
		if _, ok := pathUvMap[v.Path+":"+v.Uuid]; !ok {
			pathUvMap[v.Path+":"+v.Uuid] = v.Appid + ":" + v.Path
		}
		
	}
	
	
	//d.resultCalculation(uvMap,":UV:")
	d.resultCalculation(pathUvMap,"path_uv")
	
	
	
}

func (d dataResult) resultCalculation(data map[string]string, keyword string)  {
	// 需要去redis查询的key列表
	keyList := make([]string, len(data))
	n := 0
	for key, _ := range data {
		keyList[n] = dateStr + ":"+strings.ReplaceAll(strings.ToUpper(keyword),"_",":") +":"+ key
		n++
	}
	
	if len(keyList) == 0 {
		return
	}
	t := time.Now()
	_, err := redisClient.MGet(keyList...)
	fmt.Println("查询redis用时:",time.Now().Sub(t))
	if err != nil {
		fmt.Println("Redis MGet 错误:", err)
		return
	}
	
	//for i, v := range res {
	//	if v != nil {
	//		continue
	//	}
	//	_ = strings.Split(keyList[i], ":")[2:]
	//	//fmt.Println(keyList[i])
	//	// uvMap[key] 等于 appid:path uv +1
	//	//lock.Lock()
	//	//d.timeResult[data[strings.Join(key,":")]].pathUv++
	//	//lock.Unlock()
	//}
	
}

func main() {
	t0 := time.Now()
	t := NewComputed("/Users/lei/workspace/gitlab/vmatch-stats/test_logs/stats.access.202001081530.log")
	t.Run()
	fmt.Println("运行用时:", time.Now().Sub(t0))
}
