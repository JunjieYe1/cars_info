import requests
import json
import pymysql
from datetime import datetime, timedelta
import time
import schedule
import logging
from dbutils.pooled_db import PooledDB

# 配置日志
logging.basicConfig(filename='vehicle_track.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 数据库连接信息
# host = '111.173.89.238'
host = 'localhost'
user = 'yjj'
password = 'pass'
database = 'Cars'

# 旧接口设置
session_id = "sNRkJpZXYwF2dmdmdmgCQmNlYIN3S1Nnawhic3kWNPNjZ5JWYeFWepZCZxpnch9VYf1mYmVmdkFXby9FRfNFNvJDawEXeY"
old_base_url = "http://121.37.154.193:9999/gps-web/api/get_gps_h.jsp"

# 新接口设置
new_base_url = "http://111.173.89.238:7203/info_report/v1/query_track_data"

# 配置连接池
pool = PooledDB(
    creator=pymysql,
    host=host,
    user=user,
    password=password,
    database=database,
    autocommit=True,
    blocking=True,
    maxconnections=5
)

def get_last_update_time(cursor, vehicle_id):
    cursor.execute("SELECT MAX(track_time) FROM VehicleTrack WHERE vehicle_id = %s", (vehicle_id,))
    result = cursor.fetchone()
    return result[0] if result[0] else None

def log_error_details(error_message, data=None):
    logging.error(f"{error_message}")
    if data:
        logging.error(f"相关数据: {json.dumps(data, ensure_ascii=False)}")

def fetch_and_store_vehicle_tracks():
    connection = pool.connection()
    cursor = connection.cursor()
    try:
        # 处理老城区环卫车辆（旧接口）
        cursor.execute("SELECT id, carId FROM VehicleInfo WHERE project_category = '老城区环卫' AND carId IS NOT NULL")
        old_vehicles = cursor.fetchall()
        process_old_interface(old_vehicles, cursor)

        # 处理渣土项目车辆（新接口）
        cursor.execute("SELECT id, license_plate FROM VehicleInfo WHERE project_category = '渣土项目'")
        new_vehicles = cursor.fetchall()
        process_new_interface(new_vehicles, cursor)

        connection.commit()
    except Exception as e:
        log_error_details(f"获取或插入轨迹数据时出错: {e}")
    finally:
        cursor.close()
        connection.close()

def process_old_interface(vehicles, cursor):
    optimized_data = []

    for vehicle in vehicles:
        vehicle_id, car_id = vehicle

        if not car_id:
            logging.warning(f"车辆 ID {vehicle_id} 的 carId 为空，跳过此车辆。")
            continue

        last_update_time = get_last_update_time(cursor, vehicle_id)
        start_time = last_update_time if last_update_time else datetime.now() - timedelta(days=1)
        end_time = datetime.now()
        start_time_str = start_time.strftime('%Y%m%d%H%M%S')
        end_time_str = end_time.strftime('%Y%m%d%H%M%S')

        params = {
            "sessionId": session_id,
            "carId": car_id,
            "startTime": start_time_str,
            "endTime": end_time_str
        }

        try:
            response = requests.get(old_base_url, params=params)
            response.raise_for_status()  # 如果响应状态码不是200，将会抛出异常
            track_data = response.json().get('list', [])
            last_time = None
            for entry in track_data:
                track_time = datetime.strptime(entry['time'], '%Y-%m-%d %H:%M:%S')
                if last_time is None or (track_time - last_time).total_seconds() >= 300:
                    optimized_data.append({
                        "vehicle_id": vehicle_id,
                        "latitude": float(entry['glat']),
                        "longitude": float(entry['glng']),
                        "track_time": track_time
                    })
                    last_time = track_time
        except requests.RequestException as req_e:
            log_error_details(f"旧接口请求出错: {req_e}", data=params)
        except Exception as e:
            log_error_details(f"处理旧接口数据时出错: {e}", data=params)

    if optimized_data:
        try:
            insert_query = """
            INSERT INTO VehicleTrack (vehicle_id, latitude, longitude, track_time)
            VALUES (%s, %s, %s, %s)
            """
            data_to_insert = [(d['vehicle_id'], d['latitude'], d['longitude'], d['track_time']) for d in optimized_data]
            cursor.executemany(insert_query, data_to_insert)
            logging.info(f"{len(data_to_insert)} 条记录插入成功.")
        except Exception as e:
            log_error_details(f"插入旧接口轨迹数据时出错: {e}", data=optimized_data)
    else:
        logging.info("没有新的轨迹数据插入（旧接口）。")

def process_new_interface(vehicles, cursor):
    optimized_data = []

    for vehicle in vehicles:
        vehicle_id, license_plate = vehicle

        last_update_time = get_last_update_time(cursor, vehicle_id)
        start_time = last_update_time if last_update_time else datetime.now() - timedelta(days=1)
        end_time = datetime.now()

        params = {
            "startTime": start_time.strftime('%Y-%m-%d %H:%M:%S'),
            "endTime": end_time.strftime('%Y-%m-%d %H:%M:%S'),
            "vehicleNo": license_plate,
            "curPage": 1,
            "pageNum": 9999
        }

        try:
            response = requests.get(new_base_url, params=params)
            response.raise_for_status()
            response_data = response.json()
            if response_data['hdr']['code'] == 200 and response_data['data']:
                track_data = response_data['data']['dataList']
                last_time = None
                for entry in track_data:
                    track_time = datetime.fromtimestamp(entry['time'])
                    if last_time is None or (track_time - last_time).total_seconds() >= 300:
                        optimized_data.append({
                            "vehicle_id": vehicle_id,
                            "latitude": entry['latitude'] / 1000000.0,
                            "longitude": entry['longitude'] / 1000000.0,
                            "track_time": track_time
                        })
                        last_time = track_time
        except requests.RequestException as req_e:
            log_error_details(f"新接口请求出错: {req_e}", data=params)
        except Exception as e:
            log_error_details(f"处理新接口数据时出错: {e}", data=params)

    if optimized_data:
        try:
            insert_query = """
            INSERT INTO VehicleTrack (vehicle_id, latitude, longitude, track_time)
            VALUES (%s, %s, %s, %s)
            """
            data_to_insert = [(d['vehicle_id'], d['latitude'], d['longitude'], d['track_time']) for d in optimized_data]
            cursor.executemany(insert_query, data_to_insert)
            logging.info(f"{len(data_to_insert)} 条记录插入成功（新接口）。")
        except Exception as e:
            log_error_details(f"插入新接口轨迹数据时出错: {e}", data=optimized_data)
    else:
        logging.info("没有新的轨迹数据插入（新接口）。")

# 立即运行一次
fetch_and_store_vehicle_tracks()

# 定时任务，每5分钟执行一次
schedule.every(5).minutes.do(fetch_and_store_vehicle_tracks)

try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    logging.info("程序终止")
