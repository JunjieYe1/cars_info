# vehicle_tracker.py

import requests
import json
import pymysql
from datetime import datetime, timedelta
import time
import schedule
import logging
from dbutils.pooled_db import PooledDB
import math
from config import DB_CONFIG, SESSION_ID_OLD_URBAN, API_URLS, LOG_FILE_TRACKER

class VehicleTracker:
    def __init__(self, loop_interval=5):
        self.data_interval = 10
        self.loop_interval = loop_interval  # 循环间隔时间（分钟）

        # 创建日志记录器
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)  # 设置日志级别

        # 创建文件处理器
        file_handler = logging.FileHandler(LOG_FILE_TRACKER)
        file_handler.setLevel(logging.INFO)

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # 创建日志格式器并设置给处理器
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # 将处理器添加到日志记录器
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # 避免日志重复输出
        self.logger.propagate = False

        # 数据库连接信息
        self.host = DB_CONFIG.get('host', '111.173.89.238')
        self.user = DB_CONFIG.get('user', 'yjj')
        self.password = DB_CONFIG.get('password', 'pass')
        self.database = DB_CONFIG.get('db', 'Cars')

        # 旧接口设置
        self.session_id = SESSION_ID_OLD_URBAN
        self.old_base_url = API_URLS['old_urban_count']

        # 新接口设置
        self.new_base_url = API_URLS['new_urban_count']

        # 新城区项目接口设置
        self.new_urban_base_url = "http://220.178.1.18:8542/GPSBaseserver/stdHisAlarm/getHisGPS.do"
        self.new_urban_session_id = "57c7ccea-5e8c-493a-8ac8-db8598deadac-02333474"

        # 配置连接池
        self.pool = PooledDB(
            creator=pymysql,
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            autocommit=True,
            blocking=True,
            maxconnections=5
        )

    # 判断是否在中国范围内
    def out_of_china(self, lng, lat):
        if lng < 72.004 or lng > 137.8347:
            return True
        if lat < 0.8293 or lat > 55.8271:
            return True
        return False

    # 转换纬度
    def transform_lat(self, lng, lat):
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 * math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * math.pi) + 40.0 * math.sin(lat / 3.0 * math.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * math.pi) + 320 * math.sin(lat * math.pi / 30.0)) * 2.0 / 3.0
        return ret

    # 转换经度
    def transform_lng(self, lng, lat):
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 * math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * math.pi) + 40.0 * math.sin(lng / 3.0 * math.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * math.pi) + 300.0 * math.sin(lng / 30.0 * math.pi)) * 2.0 / 3.0
        return ret

    # WGS-84 转 GCJ-02
    def wgs84_to_gcj02(self, lng, lat):
        if self.out_of_china(lng, lat):
            return lng, lat
        dlat = self.transform_lat(lng - 105.0, lat - 35.0)
        dlng = self.transform_lng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * math.pi
        magic = math.sin(radlat)
        magic = 1 - 0.00669342162296594323 * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((6335552.717000426 * magic) / (magic * sqrtmagic) * math.pi)
        dlng = (dlng * 180.0) / (6378137.0 / sqrtmagic * math.cos(radlat) * math.pi)
        mglat = lat + dlat
        mglng = lng + dlng
        return mglng, mglat

    def get_last_update_time(self, cursor, vehicle_id):
        cursor.execute("SELECT MAX(track_time) FROM VehicleTrack WHERE vehicle_id = %s", (vehicle_id,))
        result = cursor.fetchone()
        return result[0] if result[0] else None

    def log_error_details(self, error_message, data=None):
        logging.error(f"{error_message}")
        if data:
            logging.error(f"相关数据: {json.dumps(data, ensure_ascii=False)}")

    def fetch_and_store_vehicle_tracks(self):
        connection = self.pool.connection()
        cursor = connection.cursor()
        try:
            # 处理老城区环卫车辆（旧接口）
            cursor.execute("SELECT id, carId FROM VehicleInfo WHERE project_category = '老城区环卫' AND carId IS NOT NULL")
            old_vehicles = cursor.fetchall()
            self.process_old_interface(old_vehicles, cursor)

            # 处理渣土项目车辆（新接口）
            cursor.execute("SELECT id, license_plate FROM VehicleInfo WHERE project_category = '渣土项目'")
            new_vehicles = cursor.fetchall()
            self.process_new_interface(new_vehicles, cursor)

            # 处理新城区项目车辆（新接口）
            cursor.execute("SELECT id, license_plate FROM VehicleInfo WHERE project_category = '新城区项目'")
            urban_vehicles = cursor.fetchall()
            self.process_new_urban_project_interface(urban_vehicles, cursor)

            connection.commit()
        except Exception as e:
            self.log_error_details(f"获取或插入轨迹数据时出错: {e}")
        finally:
            cursor.close()
            connection.close()

    def process_old_interface(self, vehicles, cursor):
        optimized_data = []

        for vehicle in vehicles:
            vehicle_id, car_id = vehicle

            if not car_id:
                logging.warning(f"车辆 ID {vehicle_id} 的 carId 为空，跳过此车辆。")
                continue

            last_update_time = self.get_last_update_time(cursor, vehicle_id)
            start_time = last_update_time if last_update_time and datetime.now() - last_update_time < timedelta(days=2) else datetime.now() - timedelta(days=1)
            end_time = datetime.now()
            start_time_str = start_time.strftime('%Y%m%d%H%M%S')
            end_time_str = end_time.strftime('%Y%m%d%H%M%S')

            params = {
                "sessionId": self.session_id,
                "carId": car_id,
                "startTime": start_time_str,
                "endTime": end_time_str
            }

            try:
                response = requests.get(self.old_base_url, params=params)
                response.raise_for_status()  # 如果响应状态码不是200，将会抛出异常
                track_data = response.json().get('list', [])
                last_time = None
                for entry in track_data:
                    track_time = datetime.strptime(entry['time'], '%Y-%m-%d %H:%M:%S')
                    if last_time is None or (track_time - last_time).total_seconds() >= self.data_interval:
                        optimized_data.append({
                            "vehicle_id": vehicle_id,
                            "latitude": float(entry['glat']),
                            "longitude": float(entry['glng']),
                            "track_time": track_time
                        })
                        last_time = track_time
            except requests.RequestException as req_e:
                self.log_error_details(f"旧接口请求出错: {req_e}", data=params)
            except Exception as e:
                self.log_error_details(f"处理旧接口数据时出错: {e}", data=params)

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
                self.log_error_details(f"插入旧接口轨迹数据时出错: {e}", data=optimized_data)
        else:
            logging.info("没有新的轨迹数据插入（旧接口）。")

    def process_new_interface(self, vehicles, cursor):
        optimized_data = []

        for vehicle in vehicles:
            vehicle_id, license_plate = vehicle

            last_update_time = self.get_last_update_time(cursor, vehicle_id)
            start_time = last_update_time if last_update_time and datetime.now() - last_update_time < timedelta(days=2) else datetime.now() - timedelta(days=1)
            end_time = datetime.now()

            params = {
                "startTime": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                "endTime": end_time.strftime('%Y-%m-%d %H:%M:%S'),
                "vehicleNo": license_plate,
                "curPage": 1,
                "pageNum": 9999
            }

            try:
                response = requests.get(self.new_base_url, params=params)
                response.raise_for_status()
                response_data = response.json()
                if response_data['hdr']['code'] == 200 and response_data['data']:
                    track_data = response_data['data']['dataList']
                    last_time = None
                    for entry in track_data:
                        wgs_lng = entry['longitude'] / 1000000.0
                        wgs_lat = entry['latitude'] / 1000000.0

                        gcj_lng, gcj_lat = self.wgs84_to_gcj02(wgs_lng, wgs_lat)

                        track_time = datetime.fromtimestamp(entry['time'])
                        if last_time is None or (track_time - last_time).total_seconds() >= self.data_interval:
                            optimized_data.append({
                                "vehicle_id": vehicle_id,
                                "latitude": gcj_lat,
                                "longitude": gcj_lng,
                                "track_time": track_time
                            })
                            last_time = track_time

            except requests.RequestException as req_e:
                self.log_error_details(f"新接口请求出错: {req_e}", data=params)
            except Exception as e:
                self.log_error_details(f"处理新接口数据时出错: {e}", data=params)

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
                self.log_error_details(f"插入新接口轨迹数据时出错: {e}", data=optimized_data)
        else:
            logging.info("没有新的轨迹数据插入（新接口）。")

    def process_new_urban_project_interface(self, vehicles, cursor):
        optimized_data = []

        for vehicle in vehicles:
            vehicle_id, license_plate = vehicle

            last_update_time = self.get_last_update_time(cursor, vehicle_id)
            start_time = last_update_time if last_update_time and datetime.now() - last_update_time < timedelta(days=2) else datetime.now() - timedelta(days=1)
            end_time = datetime.now()

            # 如果开始时间和结束时间不在同一个月内，调整开始时间为结束时间所在月的月初
            if start_time.month != end_time.month:
                start_time = datetime(end_time.year, end_time.month, 1, 0, 0, 0)

            params = {
                "beginTime": start_time.strftime('%Y-%m-%d %H:%M:%S'),
                "endTime": end_time.strftime('%Y-%m-%d %H:%M:%S'),
                "vehicleNum": license_plate,
                "sessionId": self.new_urban_session_id
            }

            try:
                response = requests.post(self.new_urban_base_url, json=params)
                response.raise_for_status()
                response_data = response.json()
                if response_data['resultCode'] == 0 and response_data['data']:
                    track_data = response_data['data']
                    last_time = None
                    for entry in track_data:
                        track_time = datetime.strptime(entry['gpsTime'], '%Y-%m-%d %H:%M:%S')
                        if last_time is None or (track_time - last_time).total_seconds() >= 60:
                            gcj_lng, gcj_lat = self.wgs84_to_gcj02(float(entry['lon']), float(entry['lat']))
                            optimized_data.append({
                                "vehicle_id": vehicle_id,
                                "latitude": gcj_lat,
                                "longitude": gcj_lng,
                                "track_time": track_time
                            })
                            last_time = track_time
            except requests.RequestException as req_e:
                self.log_error_details(f"新城区项目接口请求出错: {req_e}", data=params)
            except Exception as e:
                self.log_error_details(f"处理新城区项目数据时出错: {e}", data=params)

        if optimized_data:
            try:
                insert_query = """
                INSERT INTO VehicleTrack (vehicle_id, latitude, longitude, track_time)
                VALUES (%s, %s, %s, %s)
                """
                data_to_insert = [(d['vehicle_id'], d['latitude'], d['longitude'], d['track_time']) for d in optimized_data]
                cursor.executemany(insert_query, data_to_insert)
                logging.info(f"{len(data_to_insert)} 条记录插入成功（新城区项目接口）。")
            except Exception as e:
                self.log_error_details(f"插入新城区项目轨迹数据时出错: {e}", data=optimized_data)
        else:
            logging.info("没有新的轨迹数据插入（新城区项目接口）。")

    def start(self):
        # 立即运行一次
        self.fetch_and_store_vehicle_tracks()

        # 定时任务，每 loop_interval 分钟执行一次
        schedule.every(self.loop_interval).minutes.do(self.fetch_and_store_vehicle_tracks)

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            logging.info("程序终止")
