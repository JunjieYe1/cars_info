# config.py

import json

# 读取配置文件（可选）
# 您可以选择使用 JSON 配置文件来管理参数，或者直接在这里定义
# 这里直接在代码中定义配置参数

# 数据库连接信息
DB_CONFIG = {
    # 'host': '111.173.89.238',
    'host': 'localhost',
    'port': 3306,
    'user': 'yjj',
    'password': 'pass',
    'db': 'Cars',
    # 'charset': 'utf8mb4'
}

# 定义API的Session IDs
SESSION_ID_OLD_URBAN = "sNRkJpZXYwF2dmdmdmgCQmNlYIN3S1Nnawhic3kWNPNjZ5JWYeFWepZCZxpnch9VYf1mYmVmdkFXby9FRfNFNvJDawEXeY"
SESSION_ID_NEW_URBAN = "57c7ccea-5e8c-493a-8ac8-db8598deadac-02333474"

# 定义API URLs
API_URLS = {
    'old_urban_status': "http://121.37.154.193:9999/gps-web/api/get_gps_r.jsp",
    'new_urban_status': "http://220.178.1.18:8542/GPSBaseserver/stdHisAlarm/getGPS.do",
    'old_urban_count': "http://121.37.154.193:9999/gps-web/api/get_gps_h.jsp",
    'new_urban_count': "http://111.173.89.238:7203/info_report/v1/query_track_data",
    'new_urban_count_post': "http://220.178.1.18:8542/GPSBaseserver/drivingInFoProvider/getDrivingInfo.do"
}

# 定义日志文件路径
LOG_FILE_TRACKER = 'vehicle_track.log'
LOG_FILE_DAILY = 'daily_data_track.log'

# 定义异步请求的最大并发数
MAX_CONCURRENT_REQUESTS = 10
