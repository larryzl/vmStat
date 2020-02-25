#!/bin/bash

sql_dir="/home/go/src/vmStat/static/"
work_dir="/home/go/src/vmStat"

cd $work_dir

redis_host=$(cat config.yaml|grep -A 4 queue|awk -F ":" '{print $2}'|grep -v '^$'|sed -n '1p')
redis_port=$(cat config.yaml|grep -A 4 queue|awk -F ":" '{print $2}'|grep -v '^$'|sed -n '2p')
redis_db=$(cat config.yaml|grep -A 4 queue|awk -F ":" '{print $2}'|grep -v '^$'|sed -n '3p')

mysql_host=$(cat config.yaml |grep -A 5 mysql|grep ip|awk '{print $2}')
mysql_port=$(cat config.yaml |grep -A 5 mysql|grep port|awk '{print $2}')
mysql_user=$(cat config.yaml |grep -A 5 mysql|grep username|awk '{print $2}')
mysql_pwd=$(cat config.yaml |grep -A 5 mysql|grep password|awk '{print $2}')
mysql_db=$(cat config.yaml |grep -A 5 mysql|grep database|awk '{print $2}')

while [ 1 == 1 ]; do
    sql_file_name=$(/Data/apps/redis/bin/redis-cli -h $redis_host -p $redis_port -n $redis_db LPOP MYSQL_QUEUE)
    if [ "${sql_file_name}0" == 0 ];then
      sleep 10
      continue
    else
      echo $(date) "开始倒入sql文件:" ${sql_dir}/${sql_file_name}
      echo "共执行" $(wc -l ${sql_dir}/${sql_file_name}) " 行sql文件"
      time mysql -u $mysql_user -p$mysql_pwd -h$mysql_host $mysql_db < ${sql_dir}/${sql_file_name}
      echo $(date) "导入完成"
    fi
done

