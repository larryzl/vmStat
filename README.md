# vmStat
 
## 流程

1. 每分钟写入队列一个待计算的日志path
2. 主程序读取redis队列，如果有数据，则计算，没有则等待



## 数据存储

### Redis 存储

- 日期统计
    - uv:  
        格式: 日期:TIME:UV:
    - app_uv:
        格式: 日期:TIME:APP_UV:
    - path_uv:
        格式: 日期:TIME:PATH_UV:
    - path_app_uv:
        格式: 日期:TIME:PATH_APP_UV:
    - ip:
        格式: 日期:TIME:IP:
    - app_ip:
        格式: 日期:TIME:APP_IP:
- 地域统计

- 留存率

    格式:  日期:RETENTION:APPID
    
    例: `20191229:RETENTION:2`

- 每日新增用户

### 文件存储

- 留存率

    - 位置 `static/retention/日期`
    
    - 文件名 `APPID.csv`
    
    - 格式 `uid 1...uid n`