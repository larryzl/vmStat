/*
@Time       : 2019/12/30 12:06 下午
@Author     : lei
@File       : bak
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"strings"
	"vmStat/utils/ip2region"
)

func computed() error {
	data, err := parasData("./generateLog/test.log")
	if err != nil {
		return fmt.Errorf("获取Json数据错误:", err)
	}
	
	resultMap := make(map[string]map[string]int64, 10)
	/*
		uvSlices 存储UV数据，["ts:game":[uuid1,uuid2...],"ts:shop":[...]]
		1. 如果appid:path 不存在创建 "appid:path":[]
		2. 如果uuid 不存在于任何value中，则 "ts:game":[uuid]
		3. 查询redis中 20191129:UV 中每个uuid是否存在，返回对应数据
	*/
	uvSlices := make(map[string][]interface{}, 10)
	uuidSlices := make([]interface{}, 0, 100) // 用于记录uuid
	
	/*
		pathUvSlices 存储path:uv 数据，["ts:game":[uuid1,uuid2...],"ts:shop":[...]]
		1. 如果appid:path 不存在创建 "appid:path":[]
		2. 如果uuid 不存在当前value中，则 "ts:game":[uuid]
		3. 查询redis中 20191129:UV 中每个uuid是否存在，返回对应数据
	*/
	pathUvSlices := make(map[string][]interface{}, 10) // 计算path_uv
	
	/*
		appUvSlices 存储app:uv ,判断uid在这个appid下面是否出现过
	*/
	appUvSlices := make(map[string][]interface{}, 10) // app_uv
	uidSlices := make(map[string][]interface{}, 10)   // 记录appid与uid
	
	/*
		pathAppUvSlices 存储path:app:uv ,判断uid在这个appid:path下面是否出现过
	*/
	pathAppUvSlices := make(map[string][]interface{}, 10)
	
	ipSlice := make(map[string][]interface{}, 10)
	ipAll := make([]interface{}, 0, 10)
	
	appIpSlice := make(map[string][]interface{}, 10)
	
	for _, v := range data {
		timeAppPathKey := v["appid"] + ":" + v["path"]
		timeAppKey := v["appid"]
		ipInfo := region(v["ip"])
		areaAppPathKey := "AREA:" + v["appid"] + ":" + v["path"] + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
		
		if _, ok := resultMap[timeAppPathKey]; !ok {
			resultMap[timeAppPathKey] = make(map[string]int64, 5)
			resultMap[timeAppPathKey]["pv"] = 1
		} else {
			resultMap[timeAppPathKey]["pv"]++
		}
		if _, ok := resultMap[areaAppPathKey]; !ok {
			resultMap[areaAppPathKey] = make(map[string]int64, 5)
			resultMap[areaAppPathKey]["pv"] = 1
		} else {
			resultMap[areaAppPathKey]["pv"]++
		}
		
		// 计算uv
		// 初始化 uvSlices
		if _, ok := uvSlices[timeAppPathKey]; !ok {
			uvSlices[timeAppPathKey] = make([]interface{}, 0, 10)
			uvSlices[areaAppPathKey] = make([]interface{}, 0, 10)
		}
		// 判断uuid是否有过记录
		if !isInSlice(uuidSlices, v["uuid"]) {
			uuidSlices = append(uuidSlices, v["uuid"])
			uvSlices[timeAppPathKey] = append(uvSlices[timeAppPathKey], v["uuid"]) // time
			uvSlices[areaAppPathKey] = append(uvSlices[areaAppPathKey], v["uuid"]) // area
		}
		
		// 计算path_uv
		//初始化 pathUvSlices,并判断是否有uuid记录
		if _, ok := pathUvSlices[timeAppPathKey]; !ok {
			pathUvSlices[timeAppPathKey] = make([]interface{}, 0, 10)
			pathUvSlices[areaAppPathKey] = make([]interface{}, 0, 10)
			pathUvSlices[timeAppPathKey] = append(pathUvSlices[timeAppPathKey], v["uuid"])
			pathUvSlices[areaAppPathKey] = append(pathUvSlices[areaAppPathKey], v["uuid"])
		} else {
			if !isInSlice(pathUvSlices[timeAppPathKey], v["uuid"]) {
				pathUvSlices[timeAppPathKey] = append(pathUvSlices[timeAppPathKey], v["uuid"])
				pathUvSlices[areaAppPathKey] = append(pathUvSlices[areaAppPathKey], v["uuid"])
			}
		}
		
		// 计算app_uv
		//初始化appUvSlices
		if _, ok := appUvSlices[timeAppPathKey]; !ok {
			appUvSlices[timeAppPathKey] = make([]interface{}, 0, 10)
			appUvSlices[areaAppPathKey] = make([]interface{}, 0, 10)
		}
		if _, ok := uidSlices[timeAppKey]; !ok {
			//fmt.Println("uidSlices 不存在 key:",timeAppKey)
			uidSlices[timeAppKey] = make([]interface{}, 0, 10)
			uidSlices[timeAppKey] = append(uidSlices[timeAppKey], v["uid"])
			appUvSlices[timeAppPathKey] = append(appUvSlices[timeAppPathKey], v["uid"])
			appUvSlices[areaAppPathKey] = append(appUvSlices[areaAppPathKey], v["uid"])
		} else {
			//fmt.Println("uidSlices 存在 key:",timeAppKey)
			if !isInSlice(uidSlices[timeAppKey], v["uid"]) {
				uidSlices[timeAppKey] = append(uidSlices[timeAppKey], v["uid"])
				appUvSlices[timeAppPathKey] = append(appUvSlices[timeAppPathKey], v["uid"])
				appUvSlices[areaAppPathKey] = append(appUvSlices[areaAppPathKey], v["uid"])
			}
		}
		
		// 计算path_app_uv
		// 初始化pathAppUvSlices
		if _, ok := pathAppUvSlices[timeAppPathKey]; !ok {
			pathAppUvSlices[timeAppPathKey] = make([]interface{}, 0, 10)
			pathAppUvSlices[areaAppPathKey] = make([]interface{}, 0, 10)
			pathAppUvSlices[timeAppPathKey] = append(pathAppUvSlices[timeAppPathKey], v["uid"])
			pathAppUvSlices[areaAppPathKey] = append(pathAppUvSlices[areaAppPathKey], v["uid"])
		} else {
			if !isInSlice(pathAppUvSlices[timeAppPathKey], v["uid"]) {
				pathAppUvSlices[timeAppPathKey] = append(pathAppUvSlices[timeAppPathKey], v["uid"])
				pathAppUvSlices[areaAppPathKey] = append(pathAppUvSlices[areaAppPathKey], v["uid"])
			}
		}
		
		// 计算ip
		if _, ok := ipSlice[timeAppPathKey]; !ok {
			ipSlice[timeAppPathKey] = make([]interface{}, 0, 10)
			ipSlice[areaAppPathKey] = make([]interface{}, 0, 10)
		}
		if !isInSlice(ipAll, v["ip"]) {
			ipSlice[timeAppPathKey] = append(ipSlice[timeAppPathKey], v["ip"])
			ipSlice[areaAppPathKey] = append(ipSlice[areaAppPathKey], v["ip"])
		}
		// 计算app_ip
		if _, ok := appIpSlice[timeAppPathKey]; !ok {
			appIpSlice[timeAppPathKey] = make([]interface{}, 0, 10)
			appIpSlice[areaAppPathKey] = make([]interface{}, 0, 10)
			appIpSlice[timeAppPathKey] = append(appIpSlice[timeAppPathKey], v["ip"])
			appIpSlice[areaAppPathKey] = append(appIpSlice[areaAppPathKey], v["ip"])
		} else {
			if !isInSlice(appIpSlice[timeAppPathKey], v["ip"]) {
				appIpSlice[timeAppPathKey] = append(appIpSlice[timeAppPathKey], v["ip"])
				appIpSlice[areaAppPathKey] = append(appIpSlice[areaAppPathKey], v["ip"])
			}
		}
		
	}
	
	// 查询redis，返回数据
	// 返回uv数据
	
	//redisCheck(uvSlices, resultMap, "uv")
	//redisCheck(pathUvSlices, resultMap, "path_uv")
	//redisCheck(appUvSlices, resultMap, "app_uv")
	//redisCheck(pathAppUvSlices, resultMap, "path_app_uv")
	//redisCheck(ipSlice, resultMap, "ip")
	//redisCheck(appIpSlice, resultMap, "app_ip")
	//fmt.Println(resultMap)
	return nil
}

func region(ipAddr string) *ip2region.IpInfo {
	region, err := ip2region.New("./utils/ip2region/ip2region.db")
	if err != nil {
		fmt.Println(err)
	}
	ip, err := region.BtreeSearch(ipAddr)
	return &ip
}

func redisCheck(slice map[string][]interface{}, resultMap map[string]map[string]int64, key string) {
	//defer wg.Done()
	//var newUv int64
	for k, v := range slice {
		var ret int64
		var appid, path string
		if _, ok := resultMap[k]; !ok {
			resultMap[k] = make(map[string]int64, 5)
		}
		keys := strings.Split(k, ":")
		
		if len(keys) == 2 {
			appid = keys[0]
			path = keys[1]
			if len(v) > 0 {
				switch key {
				case "path_uv":
					ret, _ = redisclient.SAdd(dayPrefix+":PATH:UV:"+path, v...)
				case "uv":
					ret, _ = redisclient.SAdd(dayPrefix+":UV", v...)
				case "app_uv":
					ret, _ = redisclient.SAdd(dayPrefix+":APP:UV:"+appid, v...)
				case "path_app_uv":
					ret, _ = redisclient.SAdd(dayPrefix+":PATH:APP:UV:"+appid+":"+path, v...)
				case "ip":
					ret, _ = redisclient.SAdd(dayPrefix+":IP", v...)
				case "app_ip":
					ret, _ = redisclient.SAdd(dayPrefix+":APP:IP:"+appid, v...)
				}
				resultMap[k][key] = ret
			} else {
				resultMap[k][key] = 0
			}
		} else {
			appid = keys[1]
			path = keys[2]
			area := keys[3] + keys[4] + keys[5]
			vKeys := make([]interface{},len(v))
			for index,value := range v{
				vKeys[index] = value.(string)+"&&"+area
			}
			
			if len(v) > 0 {
				switch key {
				case "path_uv":
					ret, _ = redisclient.SAdd(dayPrefix+":AREA:PATH:UV:"+path, vKeys...)
				case "uv":
					ret, _ = redisclient.SAdd(dayPrefix+":AREA:UV", vKeys...)
				case "app_uv":
					ret, _ = redisclient.SAdd(dayPrefix+":AREA:APP:UV:"+appid, vKeys...)
				case "path_app_uv":
					ret, _ = redisclient.SAdd(dayPrefix+":AREA:PATH:APP:UV:"+appid+":"+path, vKeys...)
				case "ip":
					ret, _ = redisclient.SAdd(dayPrefix+":AREA:IP", vKeys...)
				case "app_ip":
					ret, _ = redisclient.SAdd(dayPrefix+":AREA:APP:IP:"+appid, vKeys...)
				}
				resultMap[k][key] = ret
			} else {
				resultMap[k][key] = 0
			}
		}
		
	}
}

