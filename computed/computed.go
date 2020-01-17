/*
@Time       : 2020/1/10 4:13 下午
@Author     : lei
@File       : computed
@Software   : GoLand
@Desc       :
*/
package computed

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"strings"
	"sync"
	"time"
	"vmStat/logging"
	"vmStat/utils"
	"vmStat/utils/ip2region"
)

const (
	timeTableName          = "ob_stat_time"      // 时间表
	areaTableName          = "ob_stat_time_area" // 地区表
	newUserTableName       = "ob_stat_user"      // 新用户表
	retentionRateTableName = "ob_stat_retention"
	mapLength              = 100
)

type Result struct {
	filePath    string
	dataPrefix  string
	hourPrefix  string
	sqlFileName string
}

// 基础统计信息，uv、pv、ip等
type baseStatistic struct {
	pv        int64
	uv        int64
	pathUv    int64
	appUv     int64
	pathAppUv int64
	ip        int64
	appIp     int64
}

// 统计每天新用户数量
type userStatistic struct {
	user    uint64
	appUser uint64
}

func newUserStatistic() *userStatistic {
	user, appUser := uint64(0), uint64(0)
	return &userStatistic{user: user, appUser: appUser}
}

func newStatisticItem() *baseStatistic {
	return &baseStatistic{
		pv:        0,
		uv:        0,
		pathUv:    0,
		appUv:     0,
		pathAppUv: 0,
		ip:        0,
		appIp:     0,
	}
}

// Result 构造方法
func NewResult(filePath string) *Result {
	dataPrefixArray := strings.Split(filePath, ".")
	dataPrefix := dataPrefixArray[len(dataPrefixArray)-2][:8]
	hourPrefix := dataPrefixArray[len(dataPrefixArray)-2][:10]
	sqlFileName := dataPrefixArray[len(dataPrefixArray)-2][:12] + ".sql"
	return &Result{filePath: filePath, dataPrefix: dataPrefix, hourPrefix: hourPrefix, sqlFileName: sqlFileName}
}

var (
	logger     = logging.NewConsoleLogger("debug")
	redisPool  = utils.Pool
	wg         sync.WaitGroup
	timeResult = make(map[string]*baseStatistic, 100) // 统计时间表结果
	timeKeys   = make([]interface{}, 0, 100)          // 存储时间表字段
	areaResult = make(map[string]*baseStatistic, 100) // 统计地区表结果
	areaKeys   = make([]interface{}, 0, 100)          // 存储地区表字段
	userResult = make(map[string]*userStatistic, 100) // 存储新用户结果
	userKeys   = make([]interface{}, 0, 100)          // 存储用户表字段
)

func init() {

}

func (r *Result) Run() {
	defer redisPool.Close()
	accessData, err := utils.SerData(r.filePath)
	region, _ := ip2region.New("./utils/ip2region/ip2region.db")

	uvMap := make(map[interface{}]string, 1024) // 存储uv记录 key: r.dataPrefix + ":" + v.Uuid value: v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
	uvSlice := make([]interface{}, 0, 1024)     //存储uv字段 r.dataPrefix + ":" + v.Uuid

	pathUvMap := make(map[interface{}]string, 1024)
	pathUvSlice := make([]interface{}, 0, 1024)

	appUvMap := make(map[interface{}]string, 1024)
	appUvSlice := make([]interface{}, 0, 1024)

	pathAppUvMap := make(map[interface{}]string, 1024)
	pathAppUvSlice := make([]interface{}, 0, 1024)

	ipMap := make(map[interface{}]string, 1024)
	ipSlice := make([]interface{}, 0, 1024)

	appIpMap := make(map[interface{}]string, 1024)
	appIpSlice := make([]interface{}, 0, 1024)

	userMap := make(map[string]string, 1024)    // 存储用户统计
	appUserMap := make(map[string]string, 1024) // 存储用户统计

	if err != nil {
		fmt.Println("序列化数据错误:", err)
		return
	}
	for _, v := range accessData {
		ipInfo, err := region.MemorySearch(v.Ip)
		if err != nil {
			logger.Error("解析IP地址错误:", err)
			continue
		}
		areaFieldKey := v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
		timeFieldKey := v.Appid + ":" + v.Path

		uvKey := r.dataPrefix + ":" + v.Uuid                                      // Redis存储的字段 日期+ uuid
		pathUvKey := r.dataPrefix + ":" + v.Path + ":" + v.Uuid                   // Redis存储的字段 日期+ path + uuid
		appUvKey := r.dataPrefix + ":" + v.Appid + ":" + v.Uid                    // Redis存储的字段 日期+ appid + uid
		pathAppUvKey := r.dataPrefix + ":" + v.Path + ":" + v.Appid + ":" + v.Uid // Redis存储的字段 日期+ path + appid + uid
		ipKey := r.dataPrefix + ":" + v.Ip                                        // Redis存储的字段 日期+ path + appid + uid
		appIpKey := r.dataPrefix + ":" + v.Appid + ":" + v.Ip                     // Redis存储的字段 日期+ path + appid + uid

		if _, ok := userMap[v.Uuid]; !ok {
			userMap[v.Uuid] = v.Appid
		}
		if _, ok := appUserMap[v.Appid+":"+v.Uid]; !ok {
			appUserMap[v.Appid+":"+v.Uid] = v.Appid
		}

		if _, ok := uvMap[uvKey]; !ok {
			uvMap[uvKey] = areaFieldKey
			uvSlice = append(uvSlice, uvKey)
		}

		if _, ok := pathUvMap[pathUvKey]; !ok {
			pathUvMap[pathUvKey] = areaFieldKey
			pathUvSlice = append(pathUvSlice, pathUvKey)
		}
		if _, ok := appUvMap[appUvKey]; !ok {
			appUvMap[appUvKey] = areaFieldKey
			appUvSlice = append(appUvSlice, appUvKey)
		}
		if _, ok := pathAppUvMap[pathAppUvKey]; !ok {
			pathAppUvMap[pathAppUvKey] = areaFieldKey
			pathAppUvSlice = append(pathAppUvSlice, pathAppUvKey)
		}
		if _, ok := ipMap[ipKey]; !ok {
			ipMap[ipKey] = areaFieldKey
			ipSlice = append(ipSlice, ipKey)
		}
		if _, ok := appIpMap[appIpKey]; !ok {
			appIpMap[appIpKey] = areaFieldKey
			appIpSlice = append(appIpSlice, appIpKey)
		}

		if _, ok := timeResult[timeFieldKey]; !ok {
			timeResult[timeFieldKey] = newStatisticItem()
			timeKeys = append(timeKeys, r.hourPrefix+":"+timeFieldKey)
		}
		timeResult[timeFieldKey].pv++
		if _, ok := areaResult[areaFieldKey]; !ok {
			areaResult[areaFieldKey] = newStatisticItem()
			areaKeys = append(areaKeys, r.hourPrefix+":"+areaFieldKey)
		}
		areaResult[areaFieldKey].pv++

		if _, ok := userResult[v.Appid]; !ok {
			userResult[v.Appid] = newUserStatistic()
			userKeys = append(userKeys, r.hourPrefix+":NEW_USER:"+v.Appid)
		}

	}
	wg.Add(8)
	// 计算数据
	go r.newUserCalculation(userMap, "user")
	go r.newUserCalculation(appUserMap, "app_user")
	go r.basicInfoCalculation(uvSlice, uvMap, "uv")
	go r.basicInfoCalculation(pathUvSlice, pathUvMap, "path_uv")
	go r.basicInfoCalculation(appUvSlice, appUvMap, "app_uv")
	go r.basicInfoCalculation(pathAppUvSlice, pathAppUvMap, "path_app_uv")
	go r.basicInfoCalculation(ipSlice, ipMap, "ip")
	go r.basicInfoCalculation(appIpSlice, appIpMap, "app_ip")
	wg.Wait()
	wg.Add(3)
	// 生成sql文件
	go r.generateTimeSql(timeResult, timeKeys, "time")
	go r.generateTimeSql(areaResult, areaKeys, "area")
	go r.generateUserSql(userResult, userKeys)
	wg.Wait()

	// 写入消息队列
	redisConn := redisPool.Get()
	defer redisConn.Close()
	_, err = redisConn.Do("LPush", []interface{}{"MYSQL_QUEUE", r.sqlFileName}...)
	if err != nil {
		logger.Error("Redis LPush err:", err)
		return
	}

}

// uv/pv/app_uv 等信息计算
func (r *Result) basicInfoCalculation(s []interface{}, m map[interface{}]string, kind string) {
	redisConn := redisPool.Get()
	defer func() {
		wg.Done()
		redisConn.Close()
	}()
	// 需要更新到redis中的key
	redisNewKeys := make([]interface{}, 0, 100)

	reply, err := redis.Ints(redisConn.Do("MGet", s...))
	if err != nil {
		logger.Error("Redis MGet Err:", err)
	}
	switch kind {
	case "uv":
		for i, v := range reply {
			if v == 0 {
				/*
					s[i] = uvKey == r.dataPrefix + ":" + v.Uuid
					m[s[i]] = areaFieldKey == v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
					时间表关键字 v.Appid + ":" + v.Path
					地区表关键字 areaFieldKey
				*/
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].uv++
				areaResult[m[s[i]]].uv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "path_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].pathUv++
				areaResult[m[s[i]]].pathUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "app_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].appUv++
				areaResult[m[s[i]]].appUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "path_app_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].pathAppUv++
				areaResult[m[s[i]]].pathAppUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "ip":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].ip++
				areaResult[m[s[i]]].ip++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "app_ip":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].appIp++
				areaResult[m[s[i]]].appIp++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	}
	logger.Debug("%v:redisNewKeys length:%d\n", kind, len(redisNewKeys)/2)
	if len(redisNewKeys) == 0 {
		return
	} else {
		_, err := redisConn.Do("MSet", redisNewKeys...)
		if err != nil {
			logger.Error("Redis MSet err:", err)
			return
		}
	}

}

// 格式化时间
func (r *Result) timeFormat(s string) (string, error) {
	t, err := time.Parse("2006010215", s)
	if err != nil {
		logger.Error("Format datetime err:", err)
		return "", err
	}
	return t.Format("2006-01-02 15:00:00"), nil
}

// 新用户数据计算
func (r *Result) newUserCalculation(m map[string]string, s string) {
	redisConn := redisPool.Get()

	var newKeys = []interface{}{s}
	newUserDump := make([]byte, 0, 100)
	defer func() {
		wg.Done()
		redisConn.Close()
	}()

	res, err := redis.Strings(redisConn.Do("SMembers", s))
	if err != nil {
		logger.Error("Redis SMembers UUID err:", err)
		return
	}

	redisRes := make(map[string]int, len(res))
	for i, v := range res {
		redisRes[v] = i
	}

	switch s {
	case "user":
		for k, v := range m {
			if _, ok := redisRes[k]; !ok {
				newKeys = append(newKeys, k)
				userResult[v].user++
				newUserDump = append(newUserDump, []byte(k+"\n")...)
			}
		}
	case "app_user":
		for k, v := range m {
			if _, ok := redisRes[k]; !ok {
				newKeys = append(newKeys, k)
				newUserDump = append(newUserDump, []byte(k+"\n")...)
				userResult[v].appUser++
			}
		}
	}
	if len(newKeys) > 1 {
		_, err = redisConn.Do("SAdd", newKeys...)
		if err != nil {
			logger.Error("Redis SAdd err:", err)
			return
		}

		err = utils.WriteFiles("static/"+s+".csv", newUserDump)
		if err != nil {
			logger.Error("Write Files err:", err)
			return
		}

	}

}

// 生成 ob_stat_time/ob_stat_time_area 表sql文件
func (r *Result) generateTimeSql(result map[string]*baseStatistic, keys []interface{}, kind string) {

	redisConn := redisPool.Get()
	defer func() {
		wg.Done()
		redisConn.Close()
	}()
	sqlData := ""
	datetimeField, _ := r.timeFormat(r.hourPrefix)
	newKeys := make([]interface{}, 0, 100)
	res, err := redis.Ints(redisConn.Do("MGet", keys...))
	if err != nil {
		logger.Error("Redis MGet err:", err)
		return
	}
	switch kind {
	case "time":
		for i, v := range res {
			timeFieldKey := strings.Split(keys[i].(string), ":")

			//timeFieldKey := v.Appid + ":" + v.Path
			sPtr := result[strings.Join(timeFieldKey[1:], ":")]
			if v == 0 {
				sql := fmt.Sprintf("INSERT INTO %s (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES (\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\");\n",
					timeTableName, datetimeField, timeFieldKey[2], timeFieldKey[1], sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp)
				sqlData += sql
				newKeys = append(newKeys, []interface{}{keys[i], 1}...)
			} else {
				sql := fmt.Sprintf("UPDATE  %s SET pv=pv+%d,uv=uv+%d, path_uv=path_uv+%d,app_uv=app_uv+%d,path_app_uv=path_app_uv+%d,ip=ip+%d,app_ip=app_ip+%d WHERE dattetime=\"%s\" AND path=\"%s\" AND appid=\"%s\";\n",
					timeTableName, sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp, datetimeField, timeFieldKey[2], timeFieldKey[1])
				sqlData += sql
			}
		}
	case "area":
		for i, v := range res {
			areaFieldKey := strings.Split(keys[i].(string), ":")
			//areaFieldKey := time + v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
			sPtr := result[strings.Join(areaFieldKey[1:], ":")]
			if v == 0 {
				sql := fmt.Sprintf("INSERT INTO %s (datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\",\"%d\");\n",
					areaTableName, datetimeField, areaFieldKey[2], areaFieldKey[3], areaFieldKey[4],
					areaFieldKey[2], areaFieldKey[1], sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp)
				sqlData += sql
				newKeys = append(newKeys, []interface{}{keys[i], 1}...)
			} else {
				sql := fmt.Sprintf("UPDATE  %s SET pv=pv+%d,uv=uv+%d,path_uv=path_uv+%d,app_uv=app_uv+%d,path_app_uv=path_app_uv+%d,ip=ip+%d,app_ip=app_ip+%d WHERE dattetime=\"%s\" AND path=\"%s\" AND appid=\"%s\" AND country=\"%s\" AND province=\"%s\" AND city=\"%s\";\n",
					areaTableName, sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp, datetimeField, areaFieldKey[2], areaFieldKey[1], areaFieldKey[3], areaFieldKey[4], areaFieldKey[5])
				sqlData += sql
			}
		}
	}
	if len(newKeys) != 0 {
		_, err = redisConn.Do("MSet", newKeys...)
		if err != nil {
			logger.Error("Redis MSet err:", err)
			return
		}
	}
	err = utils.WriteFiles("static/"+r.sqlFileName, []byte(sqlData))
	if err != nil {
		logger.Error("Write Files err:", err)
		return
	}
}

// 生成 ob_stat_user 表 sql文件
func (r *Result) generateUserSql(result map[string]*userStatistic, keys []interface{}) {
	redisConn := redisPool.Get()

	defer func() {
		wg.Done()
		redisConn.Close()
	}()

	sqlData := ""
	datetimeField, _ := r.timeFormat(r.hourPrefix)
	newKeys := make([]interface{}, 0, 100)

	res, err := redis.Ints(redisConn.Do("MGet", keys...))
	if err != nil {
		logger.Error("Redis MGet err:", err)
		return
	}
	for i, v := range res {
		userFieldKey := strings.Split(keys[i].(string), ":")
		sPtr := result[userFieldKey[2]]
		if v == 0 {
			sql := fmt.Sprintf("INSERT INTO %s(datetime,appid,users,app_users) VALUES(\"%s\",\"%s\",\"%d\",\"%d\");\n", newUserTableName, datetimeField, userFieldKey[2], sPtr.user, sPtr.appUser)
			newKeys = append(newKeys, []interface{}{keys[i], 1}...)
			sqlData += sql
		} else {
			sql := fmt.Sprintf("UPDATE %s SET users=users+%d,app_users=app_users+%d WHERE datetime=\"%s\" AND appid=\"%s\";\n", newUserTableName, sPtr.user, sPtr.appUser, datetimeField, userFieldKey[2])
			sqlData += sql
		}
	}
	if len(newKeys) != 0 {
		_, err = redisConn.Do("MSet", newKeys...)
		if err != nil {
			logger.Error("Redis MSet err:", err)
			return
		}
	}

	err = utils.WriteFiles("static/"+r.sqlFileName, []byte(sqlData))
	if err != nil {
		logger.Error("Write Files err:", err)
		return
	}
}