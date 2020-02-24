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
	"os"
	"path"
	"strings"
	"sync"
	"time"
	"vmStat/logging"
	"vmStat/utils"
	"vmStat/utils/ip2region"

	//"vmStat/utils/ip2region"
)

const (
	timeTableName          = "ob_stat_time"      // 时间表
	areaTableName          = "ob_stat_time_area" // 地区表
	newUserTableName       = "ob_stat_user"      // 新用户表
	retentionRateTableName = "ob_stat_retention" // 用户留存表
	mapLength              = 100
)

type Computed struct {
	filePath    string
	dataPrefix  string
	hourPrefix  string
	sqlFileName string
	timeKeys    []interface{}
	areaResult  map[string]*baseStatistic
	areaKeys    []interface{}
	userResult  map[string]*userStatistic
	userKeys    []interface{}
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

// Computed 构造方法
func New() *Computed {
	timeKeys := make([]interface{}, 0, 100)            // 存储时间表字段
	areaResult := make(map[string]*baseStatistic, 100) // 统计地区表结果
	areaKeys := make([]interface{}, 0, 100)            // 存储地区表字段
	userResult := make(map[string]*userStatistic, 100) // 存储新用户结果
	userKeys := make([]interface{}, 0, 100)            // 存储用户表字段
	return &Computed{timeKeys: timeKeys, areaResult: areaResult, areaKeys: areaKeys, userResult: userResult, userKeys: userKeys}
}

var (
	logger     = logging.NewConsoleLogger("debug")
	wg         sync.WaitGroup
	region     *ip2region.Ip2Region
	redisPool  = utils.DataPool
	queuePool  = utils.QueuePool
	timeResult = make(map[string]*baseStatistic, 100) // 统计时间表结果

)

func init() {
	if utils.Setting.Mode == "retention" {
		return
	}
	_, err := os.Stat(utils.Setting.IpFile)
	if os.IsNotExist(err) {
		logger.Error("[%s] 文件不存在,err:%v\n", utils.Setting.IpFile, err)
	}
	region, _ = ip2region.New(utils.Setting.IpFile)
	logger.Debug("加载IP库文件完成,位置:%s\n", utils.Setting.IpFile)
}

func (this *Computed) Run(filePath string) (err error) {
	//defer queuePool.Close()
	//defer redisPool.Close()
	if filePath == "" {
		redisConn := queuePool.Get()
		defer redisConn.Close()
		logName, err := redis.String(redisConn.Do("RPop", "LOG_QUEUE"))
		if err != nil {
			//logger.Error("查询Redis错误,%v\n", err)
			return err
		}
		if logName == "" {
			logger.Debug("队列中没有待计算的日志文件...\n")
			return fmt.Errorf("队列中没有待计算的日志文件")
		}
		logger.Debug("成功从队列中获取到日志文件:%s\n", logName)
		filePath = path.Join(utils.Setting.Log.Path, logName)
	}
	_, err = os.Stat(filePath)
	if os.IsNotExist(err) {
		logger.Error("文件不存在,%s\n", filePath)
		return
	}
	dataPrefixArray := strings.Split(filePath, ".")
	this.dataPrefix = dataPrefixArray[len(dataPrefixArray)-2][:8]
	this.hourPrefix = dataPrefixArray[len(dataPrefixArray)-2][:10]
	this.sqlFileName = dataPrefixArray[len(dataPrefixArray)-2][:12] + ".sql"
	//defer func() {
	//	_ = redisPool.Close()
	//	_ = queuePool.Close()
	//}()
	accessData, err := utils.SerData(filePath)

	uvMap := make(map[interface{}]string, 1024) // 存储uv记录 key: this.dataPrefix + ":" + v.Uuid value: v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
	uvSlice := make([]interface{}, 0, 1024)     //存储uv字段 this.dataPrefix + ":" + v.Uuid

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

	userMap := make(map[string]string, 1024) // 存储用户统计
	uuidSlice := make([]interface{}, 0, 1024)
	uuidSlice = append(uuidSlice, "user_temp")
	//appidUuidMap := make(map[string][]interface{}, 1024)
	appUserMap := make(map[string]string, 1024) // 存储用户统计
	uidSlice := make([]interface{}, 0, 1024)
	uidSlice = append(uidSlice, "app_user_temp")

	// 保存所有appid的切片，将APPID 存储到 日期 的集合中，以供查找该日期所有APPID使用
	appidSlice := make([]interface{}, 0, 100)
	appidSlice = append(appidSlice, this.dataPrefix)

	appidUid := make(map[string][]interface{}, 1024) // 存储每个APP对应的UID

	if err != nil {
		logger.Error("序列化数据错误:%v\n", err)
		return
	}
	for _, v := range accessData {
		if v.Uid == "" || v.Uuid == "" {
			continue
		}

		ipInfo, err := region.MemorySearch(v.Ip)
		if err != nil {
			logger.Error("解析IP地址错误:", err)
			continue
		}

		areaFieldKey := v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
		timeFieldKey := v.Appid + ":" + v.Path

		uvKey := this.dataPrefix + ":" + v.Uuid                                      // Redis存储的字段 日期+ uuid
		pathUvKey := this.dataPrefix + ":" + v.Path + ":" + v.Uuid                   // Redis存储的字段 日期+ path + uuid
		appUvKey := this.dataPrefix + ":" + v.Appid + ":" + v.Uid                    // Redis存储的字段 日期+ appid + uid
		pathAppUvKey := this.dataPrefix + ":" + v.Path + ":" + v.Appid + ":" + v.Uid // Redis存储的字段 日期+ path + appid + uid
		ipKey := this.dataPrefix + ":" + v.Ip                                        // Redis存储的字段 日期+ path + appid + uid
		appIpKey := this.dataPrefix + ":" + v.Appid + ":" + v.Ip                     // Redis存储的字段 日期+ path + appid + uid

		if _, ok := userMap[v.Uuid]; !ok { //判断uuid是否出现过
			userMap[v.Uuid] = v.Appid
			uuidSlice = append(uuidSlice, v.Uuid)
			//if _, ok := appidUuidMap[v.Appid]; !ok { // 判断appid是否出现过
			//	appidUuidMap[v.Appid] = make([]interface{}, 0, 100)
			//	appidUuidMap[v.Appid] = append(appidUuidMap[v.Appid], "user")
			//}
			//appidUuidMap[v.Appid] = append(appidUuidMap[v.Appid], v.Uuid)
		}
		if _, ok := appUserMap[v.Appid+":"+v.Uid]; !ok {
			appUserMap[v.Appid+":"+v.Uid] = v.Appid
			uidSlice = append(uidSlice, v.Appid+":"+v.Uid)
		}

		if _, ok := appidUid[v.Appid]; !ok {

			appidSlice = append(appidSlice, v.Appid)

			appidUid[v.Appid] = make([]interface{}, 1, 100)
			appidUid[v.Appid][0] = this.dataPrefix + ":RETENTION:" + v.Appid
		}
		appidUid[v.Appid] = append(appidUid[v.Appid], v.Uid)

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
			this.timeKeys = append(this.timeKeys, this.hourPrefix+":"+timeFieldKey)
		}
		timeResult[timeFieldKey].pv++
		if _, ok := this.areaResult[areaFieldKey]; !ok {
			this.areaResult[areaFieldKey] = newStatisticItem()
			this.areaKeys = append(this.areaKeys, this.hourPrefix+":"+areaFieldKey)
		}
		this.areaResult[areaFieldKey].pv++

		if _, ok := this.userResult[v.Appid]; !ok {
			this.userResult[v.Appid] = newUserStatistic()
			this.userKeys = append(this.userKeys, this.hourPrefix+":NEW_USER:"+v.Appid)
		}

	}
	// 将每个appid 下的 uid写入到redis 集合中，key格式 20200120:
	wg.Add(1)
	go func(appidSlice []interface{}) {
		redisConn := queuePool.Get()
		defer wg.Done()
		defer redisConn.Close()
		_, err = redisConn.Do("SAdd", appidSlice...)
		if err != nil {
			logger.Error("redis SAdd err:%v\n", err)
		}
	}(appidSlice)
	//
	wg.Add(1)
	go func() {
		redisConn := queuePool.Get()
		defer wg.Done()
		defer redisConn.Close()
		for _, v := range appidUid {
			// 写入redis
			_, err = redisConn.Do("SAdd", v...)
			// 写文件
			if err != nil {
				logger.Error("redis SAdd err:%v\n", err)
			}
		}
	}()
	wg.Add(8)
	// 计算数据
	go this.newUserCalculation(userMap, uuidSlice, "user")
	go this.newUserCalculation(appUserMap, uidSlice, "app_user")
	go this.basicInfoCalculation(uvSlice, uvMap, "uv")
	go this.basicInfoCalculation(pathUvSlice, pathUvMap, "path_uv")
	go this.basicInfoCalculation(appUvSlice, appUvMap, "app_uv")
	go this.basicInfoCalculation(pathAppUvSlice, pathAppUvMap, "path_app_uv")
	go this.basicInfoCalculation(ipSlice, ipMap, "ip")
	go this.basicInfoCalculation(appIpSlice, appIpMap, "app_ip")
	wg.Wait()
	wg.Add(3)
	// 生成sql文件
	go this.generateTimeSql(timeResult, this.timeKeys, "time")
	go this.generateTimeSql(this.areaResult, this.areaKeys, "area")
	go this.generateUserSql(this.userResult, this.userKeys)
	wg.Wait()

	// 写入消息队列
	func() {
		redisConn := queuePool.Get()
		defer redisConn.Close()
		_, err = redisConn.Do("LPush", []interface{}{"MYSQL_QUEUE", this.sqlFileName}...)
		if err != nil {
			logger.Error("Redis LPush err:\n", err)
			return
		}
	}()
	return nil
}

// uv/pv/app_uv 等信息计算
func (this *Computed) basicInfoCalculation(s []interface{}, m map[interface{}]string, kind string) {
	logger.Debug("正在计算:%s\n",kind)
	redisConn := redisPool.Get()
	defer func() {
		wg.Done()
		redisConn.Close()
	}()
	// 需要更新到redis中的key
	redisNewKeys := make([]interface{}, 0, 100)
	logger.Debug("Redis MGet 数量:%d\n",len(s))
	reply, err := redis.Ints(redisConn.Do("MGet", s...))
	if err != nil {
		logger.Error("Redis MGet Err:%v\n", err)
	}
	switch kind {
	case "uv":
		for i, v := range reply {
			if v == 0 {
				/*
					s[i] = uvKey == this.dataPrefix + ":" + v.Uuid
					m[s[i]] = areaFieldKey == v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
					时间表关键字 v.Appid + ":" + v.Path
					地区表关键字 areaFieldKey
				*/
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].uv++
				this.areaResult[m[s[i]]].uv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "path_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].pathUv++
				this.areaResult[m[s[i]]].pathUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "app_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].appUv++
				this.areaResult[m[s[i]]].appUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "path_app_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].pathAppUv++
				this.areaResult[m[s[i]]].pathAppUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "ip":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].ip++
				this.areaResult[m[s[i]]].ip++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "app_ip":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				timeResult[key].appIp++
				this.areaResult[m[s[i]]].appIp++
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
func (this *Computed) timeFormat(s string) (string, error) {
	t, err := time.Parse("2006010215", s)
	if err != nil {
		logger.Error("Format datetime err:", err)
		return "", err
	}
	return t.Format("2006-01-02 15:00:00"), nil
}

// 新用户数据计算
func (this *Computed) newUserCalculation(m map[string]string, slice []interface{}, s string) {
	/*
		m = {appid:[uuid....]}
		m = {appid:[uid....]}
	*/
	redisConn := queuePool.Get()
	newUserDump := make([]byte, 0, 1024)
	//var newKeys = []interface{}{s}
	//newUserDump := make([]byte, 0, 100)
	// 关闭连接
	defer func() {
		wg.Done()
		err := redisConn.Close()
		if err != nil {
			logger.Error("Redis Close err:\n", err)
		}
	}()
	// 1. 将所有uuid 写入临时的集合中
	//fmt.Println(slice)
	redisConn.Do("SAdd", slice...)
	// 2. 计算交集
	redisInter, err := redis.Strings(redisConn.Do("SInter", s, s+"_temp"))
	if err != nil {
		logger.Error("Redis SInter err:%v\n", err)
		return
	}
	//fmt.Println("Redis Inter:", redisInter)
	// 3. 删除临时集合
	redisConn.Do("Del", s+"_temp")
	// 4. 计算
	redisRes := make(map[string]int, len(redisInter))
	for _, v := range redisInter {
		redisRes[v] = 0
	}
	switch s {
	case "user":
		for k, v := range m {
			if _, ok := redisRes[k]; !ok {
				this.userResult[v].user++
				newUserDump = append(newUserDump, []byte(k+"\n")...)
			}
		}
	case "app_user":
		for k, v := range m {
			if _, ok := redisRes[k]; !ok {
				this.userResult[v].appUser++
				newUserDump = append(newUserDump, []byte(k+"\n")...)
			}
		}

	}
	err = utils.WriteFiles("static/"+s+".csv", newUserDump)
	if err != nil {
		logger.Error("Write Files err:", err)
		return
	}
	// 5. 将新用户追加到redis中
	slice[0] = s
	//fmt.Println(slice)
	redisConn.Do("SAdd", slice...)
}

// 生成 ob_stat_time/ob_stat_time_area 表sql文件
func (this *Computed) generateTimeSql(result map[string]*baseStatistic, keys []interface{}, kind string) {

	redisConn := redisPool.Get()
	defer func() {
		wg.Done()
		redisConn.Close()
	}()
	sqlData := ""
	datetimeField, _ := this.timeFormat(this.hourPrefix)
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
	err = utils.WriteFiles("static/"+this.sqlFileName, []byte(sqlData))
	if err != nil {
		logger.Error("Write Files err:", err)
		return
	}
}

// 生成 ob_stat_user 表 sql文件
func (this *Computed) generateUserSql(result map[string]*userStatistic, keys []interface{}) {
	redisConn := redisPool.Get()

	defer func() {
		wg.Done()
		redisConn.Close()
	}()

	sqlData := ""
	datetimeField, _ := this.timeFormat(this.hourPrefix)
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

	err = utils.WriteFiles("static/"+this.sqlFileName, []byte(sqlData))
	if err != nil {
		logger.Error("Write Files err:", err)
		return
	}
}

func (this *Computed) Retention() (err error) {

	fmt.Println("执行留存率计算")
	/*
		1. 获取当前时间
		2. 依次计算每日APPID交集，如果存在，则计算，不存在，则留存率为0
	*/

	timeStamp := time.Now().AddDate(0, 0, -1)

	// 前一天日期
	contrast := timeStamp.Format("20060102")

	redisConn := queuePool.Get()
	sqlData := make([]byte, 0, 1024)

	for i := 1; i <= 7; i++ {
		timeStamp = timeStamp.AddDate(0, 0, -1)
		eachTimeStr := timeStamp.Format("20060102")
		// 依次取出交集
		res, err := redis.Strings(redisConn.Do("SInter", contrast, eachTimeStr))
		fmt.Println(timeStamp.Format("20060102"), i, "天留存率计算")
		if err != nil {
			return err
		}

		for appid := range res {
			//fmt.Printf("appid:%T\n", appid)
			// 分别计算两天到appid交集
			key := fmt.Sprintf("%s:RETENTION:%d", eachTimeStr, appid)
			contrastKey := fmt.Sprintf("%s:RETENTION:%d", contrast, appid)
			num, err := redis.Int(redisConn.Do("SCard", key))
			if num == 0 {
				logger.Debug("获取到%s数量为0\n", key)
				continue
			}
			if err != nil {
				return err
			}
			eachRes, err := redis.Strings(redisConn.Do("SInter", key, contrastKey))
			if err != nil {
				return err
			}
			logger.Debug("Redis SInter :%d , %s SCard: %d\n", len(eachRes), key, num)
			var sql string
			if i == 1 {
				sql = fmt.Sprintf("insert into ob_stat_retention (datetime, appid, r1) values (\"%s\",\"%d\",\"%f\");\n", timeStamp.Format("2006-01-02"), appid, float32(len(eachRes))/float32(num))
			} else {
				sql = fmt.Sprintf("update ob_stat_retention set this%d=%f where datetime=\"%s\" and appid=%d;\n", i, float32(len(eachRes))/float32(num), timeStamp.Format("2006-01-02"), appid)
			}
			sqlData = append(sqlData, []byte(sql)...)
		}
	}
	this.sqlFileName = time.Now().Format("200601021504") + ".sql"
	err = utils.WriteFiles("static/"+this.sqlFileName, sqlData)
	if err != nil {
		logger.Error("Write Files err:", err)
		return err
	}
	_, err = redisConn.Do("LPush", []interface{}{"MYSQL_QUEUE", this.sqlFileName}...)
	if err != nil {
		logger.Error("Redis LPush err:\n", err)
		return err
	}

	return nil
}

// 20200217:RETENTION:4
