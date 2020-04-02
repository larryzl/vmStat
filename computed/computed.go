/*
@Time       : 2020/1/10 4:13 下午
@Author     : lei
@File       : computed
@Software   : GoLand
@Desc       :
*/
package computed

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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
	areaResult  map[string]*baseStatistic
	timeResult  map[string]*baseStatistic
	userResult  map[string]*userStatistic
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

type DbWorker struct {
	Dsn string
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
	timeResult := make(map[string]*baseStatistic, 100) // 统计时间表结果
	areaResult := make(map[string]*baseStatistic, 100) // 统计地区表结果
	userResult := make(map[string]*userStatistic, 100) // 存储新用户结果
	return &Computed{areaResult: areaResult, userResult: userResult, timeResult: timeResult}
}

var (
	logger    = logging.NewConsoleLogger("debug")
	wg        sync.WaitGroup
	region    *ip2region.Ip2Region
	redisPool = utils.DataPool
	queuePool = utils.QueuePool
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

func (c *Computed) Run(filePath string) (err error) {
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
	c.dataPrefix = dataPrefixArray[len(dataPrefixArray)-2][:8]
	c.hourPrefix = dataPrefixArray[len(dataPrefixArray)-2][:10]
	c.sqlFileName = dataPrefixArray[len(dataPrefixArray)-2][:12] + ".sql"
	//defer func() {
	//	_ = redisPool.Close()
	//	_ = queuePool.Close()
	//}()

	timeKeys := make([]interface{}, 0, 100) // 存储时间表字段,作为redis的key 标记已经有过记录
	areaKeys := make([]interface{}, 0, 100) // 存储地区表字段
	userKeys := make([]interface{}, 0, 100) // 存储用户表字段

	accessData, err := utils.SerData(filePath)

	uvMap := make(map[interface{}]string, 1024) // 存储uv记录 key: c.dataPrefix + ":" + v.Uuid value: v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
	uvSlice := make([]interface{}, 0, 1024)     //存储uv字段 c.dataPrefix + ":" + v.Uuid

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
	appidSlice = append(appidSlice, c.dataPrefix)

	appidUid := make(map[string][]interface{}, 1024) // 存储每个APP对应的UID

	if err != nil {
		logger.Error("序列化数据错误:%v\n", err)
		return
	}
	n := 0
	logger.Debug("开始整理日志\n")
	for _, v := range accessData {
		if v.Uid == "" || v.Uuid == "" {
			continue
		}

		ipInfo, err := region.MemorySearch(v.Ip)
		if err != nil {
			logger.Error("解析IP地址错误:", err)
			continue
		}
		n++
		areaFieldKey := v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
		timeFieldKey := v.Appid + ":" + v.Path

		uvKey := c.dataPrefix + ":UV:" + v.Uuid                                        // Redis存储的字段 日期+ uuid
		pathUvKey := c.dataPrefix + ":PUV:" + v.Path + ":" + v.Uuid                    // Redis存储的字段 日期+ path + uuid
		appUvKey := c.dataPrefix + ":AUV:" + v.Appid + ":" + v.Uid                     // Redis存储的字段 日期+ appid + uid
		pathAppUvKey := c.dataPrefix + ":PAUV:" + v.Path + ":" + v.Appid + ":" + v.Uid // Redis存储的字段 日期+ path + appid + uid
		ipKey := c.dataPrefix + ":IP:" + v.Ip                                          // Redis存储的字段 日期+ path + appid + uid
		appIpKey := c.dataPrefix + ":AIP:" + v.Appid + ":" + v.Ip                      // Redis存储的字段 日期+ path + appid + uid

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
			appidUid[v.Appid][0] = c.dataPrefix + ":RETENTION:" + v.Appid
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

		if _, ok := c.timeResult[timeFieldKey]; !ok {
			c.timeResult[timeFieldKey] = newStatisticItem()
			timeKeys = append(timeKeys, c.hourPrefix+":TIME:INSERTED:"+timeFieldKey)
		}
		c.timeResult[timeFieldKey].pv++

		if _, ok := c.areaResult[areaFieldKey]; !ok {
			c.areaResult[areaFieldKey] = newStatisticItem()
			areaKeys = append(areaKeys, c.hourPrefix+":AREA:INSERTED:"+areaFieldKey)
		}
		c.areaResult[areaFieldKey].pv++

		if _, ok := c.userResult[v.Appid]; !ok {
			c.userResult[v.Appid] = newUserStatistic()
			dayPrefix, _ := c.timeFormat(c.hourPrefix, "day")
			userKeys = append(userKeys, dayPrefix+":NEW_USER:"+v.Appid)
		}

	}
	logger.Debug("日志整理完毕，共整理%d条日志，开始计算\n", n)
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
	go c.newUserCalculation(userMap, uuidSlice, "user")
	go c.newUserCalculation(appUserMap, uidSlice, "app_user")

	go c.basicInfoCalculation(uvSlice, uvMap, "uv")
	go c.basicInfoCalculation(pathUvSlice, pathUvMap, "path_uv")
	go c.basicInfoCalculation(appUvSlice, appUvMap, "app_uv")
	go c.basicInfoCalculation(pathAppUvSlice, pathAppUvMap, "path_app_uv")
	go c.basicInfoCalculation(ipSlice, ipMap, "ip")
	go c.basicInfoCalculation(appIpSlice, appIpMap, "app_ip")
	wg.Wait()
	wg.Add(3)
	// 生成sql文件
	go c.generateNormalSql(timeKeys, "time")
	go c.generateNormalSql(areaKeys, "area")
	go c.generateNormalSql(userKeys, "user")
	wg.Wait()

	// 写入消息队列
	func() {
		redisConn := queuePool.Get()
		defer redisConn.Close()
		_, err = redisConn.Do("LPush", []interface{}{"MYSQL_QUEUE", c.sqlFileName}...)
		if err != nil {
			logger.Error("Redis LPush err:\n", err)
			return
		}
	}()
	return nil
}

// uv/pv/app_uv 等信息计算
func (c *Computed) basicInfoCalculation(s []interface{}, m map[interface{}]string, kind string) {
	redisConn := redisPool.Get()
	defer func() {
		wg.Done()
		redisConn.Close()
	}()
	// 需要更新到redis中的key
	redisNewKeys := make([]interface{}, 0, 100)
	reply, err := redis.Ints(redisConn.Do("MGet", s...))
	if err != nil {
		logger.Error("Redis MGet Err:%v\n", err)
	}
	switch kind {
	case "uv":
		for i, v := range reply {
			if v == 0 {
				/*
					s[i] = uvKey == c.dataPrefix + ":" + v.Uuid
					m[s[i]] = areaFieldKey == v.Appid + ":" + v.Path + ":" + ipInfo.Country + ":" + ipInfo.Province + ":" + ipInfo.City
					时间表关键字 v.Appid + ":" + v.Path
					地区表关键字 areaFieldKey
				*/
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				c.timeResult[key].uv++
				c.areaResult[m[s[i]]].uv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "path_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				c.timeResult[key].pathUv++
				c.areaResult[m[s[i]]].pathUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "app_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				c.timeResult[key].appUv++
				c.areaResult[m[s[i]]].appUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "path_app_uv":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				c.timeResult[key].pathAppUv++
				c.areaResult[m[s[i]]].pathAppUv++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "ip":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				c.timeResult[key].ip++
				c.areaResult[m[s[i]]].ip++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	case "app_ip":
		for i, v := range reply {
			if v == 0 {
				key := strings.Join(strings.Split(m[s[i]], ":")[:2], ":")
				c.timeResult[key].appIp++
				c.areaResult[m[s[i]]].appIp++
				redisNewKeys = append(redisNewKeys, []interface{}{s[i], 1}...)
			}
		}
	}
	//logger.Debug("%v:redisNewKeys length:%d\n", kind, len(redisNewKeys)/2)
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

// MySQL Insert数据
func (c *Computed) mysqlInsert(insertPrepare string, insertData [][]interface{}, keys []interface{}) (err error) {
	/*
		insertPrepare   sql头信息
		insertData 		写入的数据
		newKeys			redis中的insert 标记
	*/
	// MySQL 连接信息

	redisConn := redisPool.Get()
	defer redisConn.Close()

	var redisData = make([]interface{}, 0, 100)

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", utils.Setting.Mysql.Username, utils.Setting.Mysql.Password, utils.Setting.Mysql.IP, utils.Setting.Mysql.Port, utils.Setting.Mysql.Database)
	dbw := DbWorker{Dsn: dsn}
	// 建立连接
	db, err := sql.Open("mysql", dbw.Dsn)
	if err != nil {
		logger.Error("MySQL 连接错误,err:%v\n", err)
		return err
	}
	start := time.Now()
	// 开始事物
	results := make([]int64, len(insertData))
	stm, err := db.Prepare(insertPrepare)
	if err != nil {
		logger.Error("mysql prepare err:%v\n", err)
		return err
	}
	for i, d := range insertData {
		result, err := stm.Exec(d...)
		if err != nil {
			logger.Error("Mysql Stm Exec err:%v\n", err)
			return err
		}
		id, _ := result.LastInsertId()
		results[i] = id
	}
	stm.Close()
	end := time.Now()

	for i, v := range keys {
		redisData = append(redisData, v)
		redisData = append(redisData, results[i])
	}
	// 将新key写入redis
	_, _ = redisConn.Do("MSet", redisData...)

	logger.Debug("MySQL Insert rows:%d used time:%f\n", len(keys), end.Sub(start).Seconds())
	return nil
}

// 格式化时间
func (c *Computed) timeFormat(s string, k string) (string, error) {
	t, err := time.Parse("2006010215", s)
	if err != nil {
		logger.Error("Format datetime err:", err)
		return "", err
	}
	if k == "day" {
		return t.Format("2006-01-02"), nil
	} else {
		return t.Format("2006-01-02 15:00:00"), nil
	}
}

// 新用户数据计算
func (c *Computed) newUserCalculation(m map[string]string, slice []interface{}, s string) {
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
		logger.Error("%s Redis SInter err:%v\n", s, err)
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
				c.userResult[v].user++
				newUserDump = append(newUserDump, []byte(k+"\n")...)
			}
		}
	case "app_user":
		for k, v := range m {
			if _, ok := redisRes[k]; !ok {
				c.userResult[v].appUser++
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
func (c *Computed) generateNormalSql(keys []interface{}, kind string) {
	/*
		key: redis中存储的字段   例: 2020022310:appid:path , 用来判断是否有insert记录
		kind: 生成类别 time / area
	*/
	redisConn := redisPool.Get()
	defer func() {
		wg.Done()
		_ = redisConn.Close()
	}()

	var insertPrepare string                       // 存储 sql 头信息
	var updateSqlData string                       // 存储 update sql语句
	var newKeys = make([]interface{}, 0, 100)      // redis中存储新insert 的key
	var insertData = make([][]interface{}, 0, 100) // 存储insert 的数据

	res, err := redis.Ints(redisConn.Do("MGet", keys...)) // 查询redis，判断是否有insert记录

	if err != nil {
		logger.Debug("Redis MGet keys:%v\n", keys)
		logger.Error("%s Redis MGet err:%v\n", kind, err)
		return
	}

	switch kind {
	case "time":
		datetimeField, _ := c.timeFormat(c.hourPrefix, "hour") // 格式化成mysql中的时间字段
		dateField,_ := c.timeFormat(c.hourPrefix,"day")
		insertPrepare = "INSERT INTO " + timeTableName + " (datetime, path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip,date) VALUE (?,?,?,?,?,?,?,?,?,?,?);"
		for i, v := range res {
			timeFieldKey := strings.Split(keys[i].(string), ":")

			//timeFieldKey := v.Appid + ":" + v.Path
			sPtr := c.timeResult[strings.Join(timeFieldKey[3:], ":")]

			if v == 0 {
				insertData = append(insertData, []interface{}{datetimeField, timeFieldKey[4], timeFieldKey[3], sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp,dateField})
				newKeys = append(newKeys, keys[i])
			} else {
				updateSqlData += fmt.Sprintf("UPDATE  %s SET pv=pv+%d,uv=uv+%d, path_uv=path_uv+%d,app_uv=app_uv+%d,path_app_uv=path_app_uv+%d,ip=ip+%d,app_ip=app_ip+%d WHERE id=%d;\n",
					timeTableName, sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp, v)
			}
		}

	case "area":
		datetimeField, _ := c.timeFormat(c.hourPrefix, "hour") // 格式化成mysql中的时间字段
		insertPrepare = "INSERT INTO " + areaTableName + "(datetime, country,province,city,path, appid, pv, uv, path_uv, app_uv, path_app_uv,ip,app_ip) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?); "
		for i, v := range res {
			areaFieldKey := strings.Split(keys[i].(string), ":")
			sPtr := c.areaResult[strings.Join(areaFieldKey[3:], ":")]
			if v == 0 {
				insertData = append(insertData, []interface{}{datetimeField, areaFieldKey[5], areaFieldKey[6], areaFieldKey[7],
					areaFieldKey[4], areaFieldKey[3], sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp})
				//updateSqlData += sql
				newKeys = append(newKeys, keys[i])
			} else {
				updateSqlData += fmt.Sprintf("UPDATE  %s SET pv=pv+%d,uv=uv+%d,path_uv=path_uv+%d,app_uv=app_uv+%d,path_app_uv=path_app_uv+%d,ip=ip+%d,app_ip=app_ip+%d WHERE id=%d;\n",
					areaTableName, sPtr.pv, sPtr.uv, sPtr.pathUv, sPtr.appUv, sPtr.pathAppUv, sPtr.ip, sPtr.appIp, v)
			}
		}
	case "user":
		// 生成 ob_stat_user 表 sql文件
		datetimeField, _ := c.timeFormat(c.hourPrefix, "day") // 格式化成mysql中的时间字段
		insertPrepare = "INSERT INTO " + newUserTableName + " (datetime,appid,users,app_users) VALUES (?,?,?,?);"
		for i, v := range res {
			userFieldKey := strings.Split(keys[i].(string), ":")
			sPtr := c.userResult[userFieldKey[2]]
			if v == 0 {
				insertData = append(insertData, []interface{}{datetimeField, userFieldKey[2], sPtr.user, sPtr.appUser})
				newKeys = append(newKeys, keys[i])
			} else {
				updateSqlData += fmt.Sprintf("UPDATE %s SET users=users+%d,app_users=app_users+%d WHERE id=%d;\n", newUserTableName, sPtr.user, sPtr.appUser, v)
			}
		}
	}
	_ = c.mysqlInsert(insertPrepare, insertData, newKeys)
	err = utils.WriteFiles("static/"+c.sqlFileName, []byte(updateSqlData))
	if err != nil {
		logger.Error("Write Files err:", err)
		return
	}
}

// 生成留存率 sql文件
func (c *Computed) Retention() (err error) {

	fmt.Println("执行留存率计算")
	/*
		1. 获取当前时间
		2. 依次计算每日APPID交集，如果存在，则计算，不存在，则留存率为0
	*/
	// 前一天时间为基准时间,通过前n天与基准时间做比较
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
				sql = fmt.Sprintf("update ob_stat_retention set r%d=%f where datetime=\"%s\" and appid=%d;\n", i, float32(len(eachRes))/float32(num), timeStamp.Format("2006-01-02"), appid)
			}
			sqlData = append(sqlData, []byte(sql)...)
		}
	}
	c.sqlFileName = time.Now().Format("200601021504") + "_retention_.sql"
	err = utils.WriteFiles("static/"+c.sqlFileName, sqlData)
	if err != nil {
		logger.Error("Write Files err:", err)
		return err
	}
	_, err = redisConn.Do("LPush", []interface{}{"MYSQL_QUEUE", c.sqlFileName}...)
	if err != nil {
		logger.Error("Redis LPush err:\n", err)
		return err
	}

	return nil
}

// 20200217:RETENTION:4
