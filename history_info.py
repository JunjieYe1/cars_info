# daily_data_tracker.py

import asyncio
import aiomysql
import aiohttp
from datetime import datetime
import json
import logging
from config import DB_CONFIG, SESSION_ID_OLD_URBAN, SESSION_ID_NEW_URBAN, API_URLS, MAX_CONCURRENT_REQUESTS
import math

class DailyDataTracker:
    def __init__(self, loop_interval=60):
        self.loop_interval = loop_interval  # 循环间隔时间（分钟）
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # 配置日志
        logging.basicConfig(
            filename='daily_data_track.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def map_status_code(self, alarmType):
        """将alarmType转换为简化的状态码"""
        try:
            alarmType = int(alarmType)
        except (ValueError, TypeError):
            return -1  # 转换失败，返回未知状态

        if alarmType in [1, 2, 3, 4]:
            return 0  # 离线
        elif alarmType in [9, 10, 11, 12, 5, 6]:
            return 1  # 在线 - 停车
        elif alarmType in [7, 8]:
            return 2  # 在线 - 行驶
        elif alarmType == 13:
            return 3  # 在线 - 熄火
        else:
            return -1  # 未知状态

    def parse_new_urban_status(self, stateStr, speed):
        """解析新城区项目的状态字符串和速度，返回状态码"""
        if not stateStr:
            return -1  # 未知状态

        first_state = stateStr.split(',')[0]

        if first_state == "车辆点火":
            return 2 if speed > 0 else 1  # 行驶或停车
        elif first_state == "车辆熄火":
            return 3  # 熄火
        else:
            return -1  # 未知状态

    def safe_convert(self, value, to_type, default):
        """安全地将值转换为指定类型，转换失败则返回默认值"""
        try:
            return to_type(value)
        except (ValueError, TypeError):
            return default

    async def connect_db(self):
        """建立数据库连接"""
        return await aiomysql.connect(**DB_CONFIG)

    async def fetch_status_data_old_urban(self, session, session_id, start_time, end_time):
        """获取老城区环卫车辆的状态数据"""
        async with self.semaphore:
            try:
                params = {
                    'sessionId': session_id,
                    'startTime': start_time,
                    'endTime': end_time
                }
                async with session.get(
                    API_URLS['old_urban_status'],
                    params=params,
                    timeout=5
                ) as response:
                    response.raise_for_status()
                    data = await response.json(content_type=None)
                    vehicle_status_dict = {}
                    if 'list' in data:
                        for vehicle in data['list']:
                            carId = vehicle.get('carId')
                            alarmType = vehicle.get('state')
                            if carId and alarmType is not None:
                                simplified_status = self.map_status_code(alarmType)
                                vehicle_status_dict[str(carId)] = simplified_status
                    return vehicle_status_dict
            except Exception as e:
                self.logger.error(f"Error fetching old urban status data for interval {start_time} - {end_time}: {e}")
                return {}

    async def fetch_status_data_new_urban(self, session, session_id):
        """获取新城区项目车辆的状态数据"""
        async with self.semaphore:
            try:
                payload = {
                    "sessionId": session_id
                }
                async with session.post(
                    API_URLS['new_urban_status'],
                    json=payload,
                    timeout=10
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    vehicle_status_dict = {}
                    if data.get('resultCode') == 0 and 'data' in data:
                        for vehicle in data['data']:
                            vehicleNum = vehicle.get('vehicleNum')  # 车牌号
                            stateStr = vehicle.get('stateStr')      # 状态字符串
                            speed = vehicle.get('speed', 0)         # 速度，默认为0

                            # 判断是否在线
                            gpsTimeStr = vehicle.get('gpsTime')     # GPS时间
                            try:
                                gpsTime = datetime.strptime(gpsTimeStr, '%Y-%m-%d %H:%M:%S') if gpsTimeStr else None
                            except (ValueError, TypeError):
                                gpsTime = None

                            status = self.parse_new_urban_status(stateStr, speed) if gpsTime and gpsTime.date() == datetime.now().date() else 0

                            if vehicleNum:
                                vehicle_status_dict[vehicleNum] = status
                    return vehicle_status_dict
            except Exception as e:
                self.logger.error(f"Error fetching new urban status data: {e}")
                return {}

    async def fetch_count_data_old_urban(self, session, session_id, car_id, start_time, end_time):
        """获取老城区环卫车辆的计数数据"""
        async with self.semaphore:
            try:
                if not car_id:
                    return {}
                params = {
                    'sessionId': session_id,
                    'carId': car_id,
                    'startTime': start_time,
                    'endTime': end_time
                }
                async with session.get(
                    API_URLS['old_urban_count'],
                    params=params,
                    timeout=15
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return data.get('countData', {})
            except Exception as e:
                self.logger.error(f"Error fetching old urban count data for car {car_id}: {e}")
                return {}

    async def fetch_count_data_zt(self, session, license_plate, start_time, end_time):
        """获取渣土项目车辆的计数数据"""
        async with self.semaphore:
            try:
                if not license_plate:
                    return {}
                params = {
                    'vehicleNo': license_plate,
                    'startTime': start_time,
                    'endTime': end_time,
                    'curPage': 1,
                    'pageNum': 9999
                }
                async with session.get(
                    API_URLS['new_urban_count'],
                    params=params,
                    timeout=15
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if "dataList" not in data.get("data", {}):
                        return {}
                    data_list = data["data"]["dataList"]
                    if len(data_list) > 2:
                        mile = round((self.safe_convert(data_list[-1].get("deviceMileage"), float, 0.0) -
                                      self.safe_convert(data_list[0].get("deviceMileage"), float, 0.0)) / 1000, 2)
                        move_long = self.safe_convert(data_list[-1].get('driveTimeLen'), int, 0)
                        return {"mile": mile, "move_long": move_long}
                    return {}
            except Exception as e:
                self.logger.error(f"Error fetching count data for car {license_plate}: {e}")
                return {}

    async def fetch_count_data_new_urban(self, session, license_plate, start_time, end_time):
        """获取新城区项目车辆的计数数据"""
        async with self.semaphore:
            try:
                if not license_plate:
                    return {}
                payload = {
                    "userName": "ahhygs",
                    "password": "123456",
                    "vehicleNo": license_plate,
                    "sessionId": SESSION_ID_NEW_URBAN
                }
                async with session.post(
                    API_URLS['new_urban_count_post'],
                    json=payload,
                    timeout=15
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if data.get('resultCode') == 0 and data.get('data') and data['data'][0]:
                        data_con = data['data'][0]
                        mile = self.safe_convert(data_con.get("operatingMileage"), float, 0.0)
                        move_long = self.safe_convert(data_con.get("drivingDuration"), int, 0)
                        stop_long = self.safe_convert(data_con.get("parkingDuration"), int, 0)
                        engine_off_long = self.safe_convert(data_con.get("shutdownDuration"), int, 0)
                        return {"mile": mile, "move_long": move_long, "stop_long_num": stop_long, "engine_off_long": engine_off_long}
                return {}
            except Exception as e:
                self.logger.error(f"Error fetching new urban count data for car {license_plate}: {e}")
                return {"mile": 0.0, "move_long": 0, "stop_long_num": 0, "engine_off_long": 0}

    async def fetch_all_status_data(self, session, start_time, end_time):
        """获取所有车辆的状态数据"""
        tasks = [
            self.fetch_status_data_old_urban(session, SESSION_ID_OLD_URBAN, start_time, end_time),
            self.fetch_status_data_new_urban(session, SESSION_ID_NEW_URBAN)
        ]
        results = await asyncio.gather(*tasks)
        combined_status = {**results[0], **results[1]}
        return combined_status

    async def fetch_vehicle_info(self, connection):
        """从数据库获取所有车辆信息"""
        async with connection.cursor() as cursor:
            await cursor.execute("""
                SELECT 
                    id, license_plate, carId, vehicle_group, project_category, terminal_model, terminal_number,
                    brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type, driver
                FROM vehicleinfo
            """)
            vehicles = await cursor.fetchall()
        return vehicles

    async def insert_daily_data(self, connection, daily_data):
        """将每日数据插入到数据库中"""
        if not daily_data:
            self.logger.info("No valid data to insert.")
            return
        async with connection.cursor() as cursor:
            insert_query = """
                INSERT INTO vehicle_daily_data 
                    (vehicle_id, license_plate, date, running_mileage, driving_duration, parking_duration, engine_off_duration, current_status)
                VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s) AS new
                ON DUPLICATE KEY UPDATE
                    running_mileage = new.running_mileage,
                    driving_duration = new.driving_duration,
                    parking_duration = new.parking_duration,
                    engine_off_duration = new.engine_off_duration,
                    current_status = new.current_status,
                    updated_at = CURRENT_TIMESTAMP
            """
            await cursor.executemany(insert_query, daily_data)
        await connection.commit()
        self.logger.info(f"Successfully inserted daily data for {len(daily_data)} vehicles.")

    async def process_vehicle_data(self, vehicle, count_data, status_data, today):
        """处理单个车辆的数据，返回每日数据元组或 None"""
        vehicle_id, license_plate, car_id, vehicle_group, project_category, terminal_model, terminal_number, \
            brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type, driver = vehicle

        mile = 0.0
        driving_duration = 0
        parking_duration = 0
        engine_off_duration = 0  # 初始化时长
        status = -1  # 默认状态
        data_valid = True  # 数据有效性标志

        if project_category == "老城区环卫" and car_id:
            # 处理老城区环卫项目
            mile = self.safe_convert(count_data.get('mile', '0.0'), float, 0.0)
            driving_duration = self.safe_convert(count_data.get('move_long_num', '0'), int, 0)
            parking_duration = self.safe_convert(count_data.get('stop_long_num', '0'), int, 0)
            # 检查数据有效性
            if mile < 0 or driving_duration < 0 or parking_duration < 0:
                data_valid = False
            # 获取状态
            status = status_data.get(str(car_id), -1)

        elif project_category == "老城区环卫" and not car_id:
            # 处理老城区环卫项目 新接口
            mile = self.safe_convert(count_data.get('mile', '0.0'), float, 0.0)
            driving_duration = self.safe_convert(count_data.get('move_long', 0), int, 0)

            # 根据 mile 设置状态
            status = 1 if mile > 0 else 0

        elif project_category == "渣土项目" and license_plate:
            # 处理渣土项目
            mile = self.safe_convert(count_data.get('mile', '0.0'), float, 0.0)
            driving_duration = self.safe_convert(count_data.get('move_long', 0), int, 0)

            # 根据 mile 设置状态
            status = 1 if mile > 0 else 0

        elif project_category == "新城区项目" and license_plate:
            # 处理新城区项目
            mile = self.safe_convert(count_data.get('mile', '0.0'), float, 0.0)/10
            driving_duration = self.safe_convert(count_data.get('move_long', 0), int, 0)
            parking_duration = self.safe_convert(count_data.get('stop_long', 0), int, 0)
            engine_off_duration = self.safe_convert(count_data.get('engine_off_long', 0), int, 0)

            # 获取状态
            status = status_data.get(license_plate, -1)

        else:
            # 其他项目或数据缺失，保持默认值
            mile = 0.0
            driving_duration = 0
            parking_duration = 0
            engine_off_duration = 0
            status = -1  # 未知状态

        if data_valid:
            return (
                vehicle_id,
                license_plate,
                today,
                mile,
                driving_duration,
                parking_duration,
                engine_off_duration,
                status  # 添加当前状态
            )
        else:
            self.logger.warning(f"Skipped updating data for vehicle ID {vehicle_id} due to invalid API data.")
            return None

    async def process_and_store_daily_data(self):
        """主函数，处理并存储每日数据"""
        connection = await self.connect_db()
        try:
            # 获取所有车辆信息
            vehicles = await self.fetch_vehicle_info(connection)

            async with aiohttp.ClientSession() as session:
                # 获取当前日期
                today = datetime.now().date()
                start_time_str = today.strftime('%Y%m%d000000')
                end_time_str = today.strftime('%Y%m%d235959')
                start_time_iso = today.strftime('%Y-%m-%d 00:00:00')
                end_time_iso = today.strftime('%Y-%m-%d 23:59:59')

                # 获取所有车辆的状态数据，传递时间参数
                status_data = await self.fetch_all_status_data(session, start_time_str, end_time_str)

                daily_data = []
                tasks = []

                for vehicle in vehicles:
                    vehicle_id, license_plate, car_id, vehicle_group, project_category, terminal_model, terminal_number, \
                        brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type, driver = vehicle

                    # 根据不同项目类别获取对应的轨迹数据
                    if car_id and project_category == "老城区环卫":
                        tasks.append(
                            self.fetch_count_data_old_urban(session, SESSION_ID_OLD_URBAN, car_id, start_time_str, end_time_str)
                        )
                    elif not car_id and project_category == "老城区环卫":
                        tasks.append(
                            self.fetch_count_data_zt(session, license_plate, start_time_iso, end_time_iso)
                        )
                    elif project_category == "渣土项目":
                        tasks.append(
                            self.fetch_count_data_zt(session, license_plate, start_time_iso, end_time_iso)
                        )
                    elif project_category == "新城区项目":
                        tasks.append(
                            self.fetch_count_data_new_urban(session, license_plate, start_time_iso, end_time_iso)
                        )
                    else:
                        tasks.append(asyncio.sleep(0, result={}))

                # 并发执行所有数据获取任务
                count_results = await asyncio.gather(*tasks)

                # 处理每辆车的数据
                for vehicle, count_data in zip(vehicles, count_results):
                    if not count_data:
                        # self.logger.info(f"No count data for vehicle: {vehicle[1]}")
                        continue

                    processed_data = await self.process_vehicle_data(vehicle, count_data, status_data, today)
                    if processed_data:
                        # print(processed_data)
                        daily_data.append(processed_data)

                # 插入数据到数据库
                await self.insert_daily_data(connection, daily_data)

        except Exception as e:
            self.logger.error(f"Error processing and storing daily data: {e}")
        finally:
            connection.close()

    async def run(self):
        """运行主循环，按指定间隔执行任务"""
        while True:
            self.logger.info("Starting daily data processing task.")
            await self.process_and_store_daily_data()
            self.logger.info(f"Task completed. Sleeping for {self.loop_interval} minutes.")
            await asyncio.sleep(self.loop_interval * 60)

    def start(self):
        """启动异步任务"""
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            self.logger.info("DailyDataTracker 程序终止")
