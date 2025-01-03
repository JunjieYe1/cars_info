import time

from flask import Flask, jsonify, request
import aiomysql
from datetime import datetime, timedelta
from flask_cors import CORS
import aiohttp
import asyncio
from flask_caching import Cache
import json
import threading
from database_updater import main as start_all_trackers
from vehicle_tracker import VehicleTracker
from config import DB_CONFIG
import requests
from session_manager import SessionManager

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# 配置缓存
cache = Cache(app, config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 10})

# 数据库连接信息
if DB_CONFIG:
    db_config = DB_CONFIG
else:
    db_config = {
        # 'host': '111.173.89.238',
        'host': 'localhost',
        'user': 'yjj',
        'password': 'pass',
        'db': 'Cars'
    }

# 实例化 SessionManager
session_manager = SessionManager()


async def connect_db():
    return await aiomysql.connect(**db_config)


def make_cache_key():
    date_str = request.args.get('date', 'default')
    return f"{request.path}?date={date_str}"


# 实例化 VehicleTracker
vehicle_tracker = VehicleTracker(loop_interval=5)  # 根据需要调整循环间隔时间


# 启动 SessionManager 的异步任务
def start_session_manager():
    print("Starting SessionManager...")
    while True:
        session_id = session_manager.get_session_id()
        print(f"当前 session_id: {session_id}")
        # 每10小时刷新一次
        time.sleep(36000)  # 10小时 = 36000秒

def start_async_task():
    # 在后台线程中运行异步任务
    loop = asyncio.new_event_loop()  # 为线程创建一个新的事件循环
    asyncio.set_event_loop(loop)  # 设置当前线程的事件循环
    loop.run_until_complete(start_session_manager())  # 启动异步任务


@app.route('/api/last_locations', methods=['GET'])
@cache.cached(timeout=5, key_prefix=make_cache_key)
async def get_last_locations():
    date_str = request.args.get('date')
    license_plate = request.args.get('license_plate')

    if not date_str:
        return jsonify({"error": "Date parameter is required"}), 400

    try:
        date = datetime.strptime(date_str, '%Y%m%d').date()
    except ValueError:
        return jsonify({"error": "Invalid date format, expected YYYYMMDD"}), 400

    # 如果提供了车牌号，调用 VehicleTracker 来更新该车辆的轨迹
    if license_plate:
        try:
            # 使用 asyncio.to_thread 来在后台线程中运行同步方法
            await asyncio.to_thread(vehicle_tracker.fetch_track_by_license_plate, license_plate)
            print(f"Successfully updated track for license plate: {license_plate}")
        except Exception as e:
            print(f"Error updating track for license plate {license_plate}: {e}")

    connection = await connect_db()
    cursor = await connection.cursor()

    try:
        # 查询所有车辆信息
        await cursor.execute("""
            SELECT 
                id, license_plate, carId, vehicle_group, project_category, terminal_model, terminal_number,
                brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, 
                vehicle_type, driver, driver_phone, car_no
            FROM VehicleInfo
        """)
        vehicles = await cursor.fetchall()

        if not vehicles:
            return jsonify({"error": "No vehicles found"}), 404

        vehicle_ids = [vehicle[0] for vehicle in vehicles]

        # 构建IN查询的占位符
        in_placeholders = ','.join(['%s'] * len(vehicle_ids))

        # 查询vehicle_daily_data表中的数据
        await cursor.execute(f"""
            SELECT vehicle_id, running_mileage, driving_duration, parking_duration, engine_off_duration, current_status
            FROM vehicle_daily_data
            WHERE vehicle_id IN ({in_placeholders}) AND date = %s
        """, (*vehicle_ids, date))
        daily_data = await cursor.fetchall()

        # 构建vehicle_id到daily_data的映射
        daily_data_dict = {row[0]: {
            'running_mileage': float(row[1]),
            'driving_duration': row[2],
            'parking_duration': row[3],
            'engine_off_duration': row[4],
            'current_status': row[5]
        } for row in daily_data}

        # 创建异步任务处理每辆车的数据
        tasks = [handle_vehicle_data(vehicle, daily_data_dict.get(vehicle[0])) for vehicle in vehicles]
        results = await asyncio.gather(*tasks)

    except Exception as e:
        print(f"Error in get_last_locations: {e}")
        return jsonify({"error": "Internal server error"}), 500

    finally:
        await cursor.close()
        connection.close()

    return jsonify(results)


@app.route('/api/last_locations_person', methods=['GET'])
@cache.cached(timeout=5, key_prefix=make_cache_key)
async def get_last_locations_person():
    date_str = request.args.get('date')
    license_plate = request.args.get('license_plate')

    if not date_str:
        return jsonify({"error": "Date parameter is required"}), 400

    try:
        date = datetime.strptime(date_str, '%Y%m%d').date()
    except ValueError:
        return jsonify({"error": "Invalid date format, expected YYYYMMDD"}), 400

    # 如果提供了车牌号，调用 VehicleTracker 来更新该车辆的轨迹
    if license_plate:
        try:
            # 使用 asyncio.to_thread 来在后台线程中运行同步方法
            await asyncio.to_thread(vehicle_tracker.fetch_track_by_license_plate, license_plate, True)
            print(f"Successfully updated track for license plate: {license_plate}")
        except Exception as e:
            print(f"Error updating track for license plate {license_plate}: {e}")

    connection = await connect_db()
    cursor = await connection.cursor()

    try:
        # 查询所有车辆信息
        await cursor.execute("""
            SELECT Company, PersonnelID, BadgeNumber, Name, Gender, Age, PhoneNumber, Position, HomeAddress  FROM personnel
        """)
        vehicles = await cursor.fetchall()

        if not vehicles:
            return jsonify({"error": "No vehicles found"}), 404

        vehicle_ids = [vehicle[1] for vehicle in vehicles]

        # 构建IN查询的占位符
        in_placeholders = ','.join(['%s'] * len(vehicle_ids))

        # 查询vehicle_daily_data表中的数据
        await cursor.execute(f"""
            SELECT vehicle_id, running_mileage, driving_duration, parking_duration, engine_off_duration, current_status
            FROM vehicle_daily_data
            WHERE vehicle_id IN ({in_placeholders}) AND date = %s
        """, (*vehicle_ids, date))
        daily_data = await cursor.fetchall()

        # 构建vehicle_id到daily_data的映射
        daily_data_dict = {row[0]: {
            'running_mileage': float(row[1]),
            'driving_duration': row[2],
            'parking_duration': row[3],
            'engine_off_duration': row[4],
            'current_status': row[5]
        } for row in daily_data}


        # 创建异步任务处理每辆车的数据
        tasks = [handle_person_data(vehicle, daily_data_dict.get(vehicle[1])) for vehicle in vehicles]
        results = await asyncio.gather(*tasks)

    except Exception as e:
        print(f"Error in get_last_locations: {e}")
        return jsonify({"error": "Internal server error"}), 500

    finally:
        await cursor.close()
        connection.close()

    return jsonify(results)



# 处理单个车辆数据的异步函数
async def handle_vehicle_data(vehicle, daily_data):
    (vehicle_id, license_plate, car_id, vehicle_group, project_category, terminal_model, terminal_number, \
     brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type,
     driver, driver_phone, car_no) = vehicle

    # 获取当天最后的轨迹信息
    track = await fetch_track_info(vehicle_id, daily_data)

    # 初始化返回数据
    result = {
        'id': vehicle_id,
        'license_plate': license_plate,
        'carId': car_id,
        'vehicle_group': vehicle_group,
        'project_category': project_category,
        'terminal_model': terminal_model,
        'terminal_number': terminal_number,
        'status': daily_data['current_status'] if daily_data and daily_data['current_status'] > 0 else 0,
        'latitude': track['latitude'],
        'longitude': track['longitude'],
        'last_time': track['last_time'],
        'move_long': format_duration(daily_data['driving_duration']) if daily_data else 'N/A',
        'move_long_num': daily_data['driving_duration'] if daily_data else 'N/A',
        'mile': daily_data['running_mileage'] if daily_data else 'N/A',
        'brand_model': brand_model,
        'vehicle_identification_number': vehicle_identification_number,
        'engine_number': engine_number,
        'owner': owner,
        'vehicle_name': vehicle_name,
        'gross_weight': gross_weight,
        'vehicle_type': vehicle_type,
        'driver': driver,
        'driver_phone': driver_phone,
        'car_no': car_no,
    }

    return result

# 处理单个车辆数据的异步函数
async def handle_person_data(person, daily_data):
    (Company, PersonnelID, BadgeNumber, Name, Gender, Age, PhoneNumber, Position, HomeAddress) = person

    # 获取当天最后的轨迹信息
    track = await fetch_track_info(PersonnelID, daily_data)

    # 初始化返回数据
    result = {
        'id': PersonnelID,
        'license_plate': BadgeNumber,
        'status': daily_data['current_status'] if daily_data and daily_data['current_status'] > 0 else 0,
        'latitude': track['latitude'],
        'longitude': track['longitude'],
        'last_time': track['last_time'],
        'move_long': format_duration(daily_data['driving_duration']) if daily_data else 'N/A',
        'move_long_num': daily_data['driving_duration'] if daily_data else 'N/A',
        'mile': daily_data['running_mileage'] if daily_data else 'N/A',
        'person_name': Name,
        'person_phone': PhoneNumber,
        'Age': Age,
        'Gender': Gender,
        'Position': Position,
        'HomeAddress': HomeAddress,
        'Company': Company,

    }

    return result

# 格式化持续时间为 "HH时MM分SS秒"
def format_duration(seconds):
    if not isinstance(seconds, int):
        return 'N/A'
    hours, remainder = divmod(seconds, 3600)
    minutes, sec = divmod(remainder, 60)
    formatted = ""
    if hours > 0:
        formatted += f"{hours}时"
    if minutes > 0:
        formatted += f"{minutes}分"
    formatted += f"{sec}秒"
    return formatted


# Fetch track info separately to avoid sharing the cursor concurrently
async def fetch_track_info(vehicle_id, daily_data):
    if not daily_data:
        return {'latitude': None, 'longitude': None, 'last_time': None}

    connection = await connect_db()
    cursor = await connection.cursor()
    try:
        await cursor.execute("""
            SELECT latitude, longitude, track_time
            FROM VehicleTrack
            WHERE vehicle_id = %s AND DATE(track_time) = %s
            ORDER BY track_time DESC
            LIMIT 1
        """, (vehicle_id, daily_data['date'] if 'date' in daily_data else datetime.now().date()))
        track = await cursor.fetchone()
    except Exception as e:
        print(f"Error fetching track info for vehicle {vehicle_id}: {e}")
        track = None
    finally:
        await cursor.close()
        connection.close()

    if track:
        latitude, longitude, track_time = track
        return {
            'latitude': latitude,
            'longitude': longitude,
            'last_time': track_time.strftime('%Y-%m-%d %H:%M:%S') if track_time else None
        }
    else:
        return {'latitude': None, 'longitude': None, 'last_time': None}


# Process track information to separate data logic
def process_track_info(track):
    if track:
        status = 1
        latitude = track[0]
        longitude = track[1]
        last_time = track[2].strftime('%Y-%m-%d %H:%M:%S')
    else:
        status = 0
        latitude = None
        longitude = None
        last_time = None
    return status, latitude, longitude, last_time


@app.route('/api/vehicle_tracks', methods=['GET'])
async def get_vehicle_tracks():
    vehicle_id = request.args.get('vehicle_id')
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({"error": "Date parameter is required"}), 400

    try:
        date = datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400

    connection = await connect_db()
    cursor = await connection.cursor()
    await cursor.execute("""
        SELECT latitude, longitude, track_time
        FROM VehicleTrack
        WHERE vehicle_id = %s AND DATE(track_time) = %s
        ORDER BY track_time
    """, (vehicle_id, date))
    tracks = await cursor.fetchall()
    await cursor.close()
    connection.close()

    response = [{'latitude': track[0], 'longitude': track[1], 'time': track[2].strftime('%Y-%m-%d %H:%M:%S')} for track
                in tracks]
    return jsonify(response)


@app.route('/api/save_fence', methods=['POST'])
async def save_fence():
    data = request.get_json()
    fences = data.get('fences', [])

    connection = await connect_db()
    cursor = await connection.cursor()

    try:
        # 先清空数据库中的所有围栏信息
        await cursor.execute("DELETE FROM FencePoints")
        await cursor.execute("DELETE FROM Fences")
        await connection.commit()

        if not fences:  # 即使没有围栏，也允许保存
            return jsonify({"success": True, "message": "没有围栏数据，但已清除现有的围栏信息。"})

        for i, fence in enumerate(fences):
            name = f"围栏 {i + 1}"

            await cursor.execute("INSERT INTO Fences (name, created_at) VALUES (%s, %s)", (name, datetime.now()))
            fence_id = cursor.lastrowid

            for j, point in enumerate(fence):
                await cursor.execute("""
                    INSERT INTO FencePoints (fence_id, latitude, longitude, point_order) 
                    VALUES (%s, %s, %s, %s)
                """, (fence_id, point['lat'], point['lng'], j))

        await connection.commit()
        return jsonify({"success": True, "message": "围栏已成功保存。"})

    except Exception as e:
        await connection.rollback()
        print(f"Error saving fence: {e}")
        return jsonify({"error": "围栏保存失败。"}), 500

    finally:
        await cursor.close()
        connection.close()


@app.route('/api/get_fences', methods=['GET'])
async def get_fences():
    connection = await connect_db()
    cursor = await connection.cursor()

    try:
        await cursor.execute("""
            SELECT F.id, F.name, FP.latitude, FP.longitude, FP.point_order
            FROM Fences F
            JOIN FencePoints FP ON F.id = FP.fence_id
            ORDER BY F.id, FP.point_order
        """)
        results = await cursor.fetchall()

        fences = {}
        for row in results:
            fence_id = row[0]
            name = row[1]
            lat = row[2]
            lng = row[3]
            point_order = row[4]

            if fence_id not in fences:
                fences[fence_id] = {
                    'name': name,
                    'points': []
                }

            fences[fence_id]['points'].append({
                'lat': lat,
                'lng': lng,
                'order': point_order
            })

        response = [{'id': fence_id, 'name': fence['name'], 'points': fence['points']} for fence_id, fence in
                    fences.items()]
        return jsonify(response)

    except Exception as e:
        print(f"Error retrieving fences: {e}")
        return jsonify({"error": "Failed to retrieve fences"}), 500

    finally:
        await cursor.close()
        connection.close()


@app.route('/api/get_video_url', methods=['POST'])
async def get_video_url():
    data = request.json
    vehicle_num = data.get('vehicleNum')
    if not vehicle_num:
        return jsonify({"error": "Vehicle number is required"}), 400

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    'http://220.178.1.18:8542/GPSBaseserver/videoUrlProvider/getVideoUrl.do',
                    json={
                        "userName": "ahhygs",
                        "password": "Hyhw240720@hy",
                        "vehicleNum": vehicle_num,
                        "sessionId": "57c7ccea-5e8c-493a-8ac8-db8598deadac-02333474"
                    }
            ) as response:
                response.raise_for_status()
                result = await response.json()
                return jsonify(result)
    except Exception as e:
        print(f"Error fetching video URLs for vehicle {vehicle_num}: {e}")
        return jsonify({"error": "Failed to fetch video URLs"}), 500


# 新增接口：/api/historical_data
@app.route('/api/historical_data', methods=['GET'])
async def get_historical_data():
    # 获取查询参数
    start_date_str = request.args.get('startDate')
    end_date_str = request.args.get('endDate')
    companies_str = request.args.get('companies')
    license_plates_str = request.args.get('licensePlates')

    # 验证参数是否存在
    if not start_date_str or not end_date_str or not companies_str:
        return jsonify({"error": "startDate, endDate, and companies parameters are required"}), 400

    # 解析日期
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        return jsonify({"error": "Invalid date format, expected YYYY-MM-DD"}), 400

    if start_date > end_date:
        return jsonify({"error": "startDate cannot be after endDate"}), 400

    # 解析companies
    companies = [company.strip() for company in companies_str.split(',') if company.strip()]
    if not companies:
        return jsonify({"error": "At least one company must be specified"}), 400

    # 解析license_plates
    license_plates = [plate.strip() for plate in license_plates_str.split(',')] if license_plates_str else []

    connection = await connect_db()
    cursor = await connection.cursor()

    try:
        # 如果提供了车牌号，则查询每日数据
        if license_plates:
            # 构建IN查询的占位符
            company_placeholders = ','.join(['%s'] * len(companies))
            license_plate_placeholders = ','.join(['%s'] * len(license_plates))
            params = [start_date, end_date, *companies, *license_plates]

            query = f"""
                SELECT 
                    vi.license_plate, 
                    vi.project_category, 
                    vi.driver,
                    vi.driver_phone,
                    vi.vehicle_type,
                    vi.vehicle_name,
                    vdd.date,
                    COALESCE(vdd.running_mileage, 0) AS running_mileage,
                    COALESCE(vdd.driving_duration, 0) AS driving_duration,
                    COALESCE(vdd.parking_duration, 0) AS parking_duration,
                    COALESCE(vdd.engine_off_duration, 0) AS engine_off_duration
                FROM vehicleinfo vi
                LEFT JOIN vehicle_daily_data vdd 
                    ON vi.id = vdd.vehicle_id 
                    AND vdd.date BETWEEN %s AND %s
                WHERE vi.project_category IN ({company_placeholders})
                AND vi.license_plate IN ({license_plate_placeholders})
                ORDER BY vi.license_plate, vdd.date
            """

            await cursor.execute(query, params)
            results = await cursor.fetchall()

            # 构建响应数据
            historical_data = []
            for row in results:
                license_plate, project_category, driver, driver_phone, vehicle_type, vehicle_name, date, running_mileage, driving_duration, parking_duration, engine_off_duration = row
                historical_data.append({
                    'license_plate': license_plate,
                    'project_category': project_category,
                    'driver': driver,
                    'driver_phone': driver_phone,
                    'vehicle_type': vehicle_type,
                    'vehicle_name': vehicle_name,
                    'date': date.strftime('%Y-%m-%d') if date else 'N/A',
                    'running_mileage': float(running_mileage) if running_mileage else 0.0,
                    'driving_duration': int(driving_duration) if driving_duration else 0,
                    'parking_duration': int(parking_duration) if parking_duration else 0,
                    'engine_off_duration': int(engine_off_duration) if engine_off_duration else 0
                })

        # 如果没有提供车牌号，则进行统计查询
        else:
            # 构建IN查询的占位符
            company_placeholders = ','.join(['%s'] * len(companies))
            params = [start_date, end_date, *companies]

            query = f"""
                SELECT 
                    vi.license_plate, 
                    vi.project_category, 
                    vi.driver,
                    vi.driver_phone,
                    vi.vehicle_type,
                    vi.vehicle_name,
                    COALESCE(SUM(vdd.running_mileage), 0) AS total_running_mileage,
                    COALESCE(SUM(vdd.driving_duration), 0) AS total_driving_duration,
                    COALESCE(SUM(vdd.parking_duration), 0) AS total_parking_duration,
                    COALESCE(SUM(vdd.engine_off_duration), 0) AS total_engine_off_duration
                FROM vehicleinfo vi
                LEFT JOIN vehicle_daily_data vdd 
                    ON vi.id = vdd.vehicle_id 
                    AND vdd.date BETWEEN %s AND %s
                WHERE vi.project_category IN ({company_placeholders})
                GROUP BY vi.license_plate, vi.project_category, vi.driver, vi.driver_phone, vi.vehicle_type, vi.vehicle_name
                ORDER BY vi.license_plate, vi.project_category
            """

            await cursor.execute(query, params)
            results = await cursor.fetchall()

            # 构建响应数据
            historical_data = []
            for row in results:
                license_plate, project_category, driver, driver_phone, vehicle_type, vehicle_name, running_mileage, driving_duration, parking_duration, engine_off_duration = row
                historical_data.append({
                    'license_plate': license_plate,
                    'project_category': project_category,
                    'driver': driver,
                    'driver_phone': driver_phone,
                    'vehicle_type': vehicle_type,
                    'vehicle_name': vehicle_name,
                    'running_mileage': float(running_mileage) if running_mileage else 0.0,
                    'driving_duration': int(driving_duration) if driving_duration else 0,
                    'parking_duration': int(parking_duration) if parking_duration else 0,
                    'engine_off_duration': int(engine_off_duration) if engine_off_duration else 0
                })

    except Exception as e:
        print(f"Error in get_historical_data: {e}")
        return jsonify({"error": "Internal server error"}), 500

    finally:
        await cursor.close()
        connection.close()

    return jsonify(historical_data)


@app.route('/api/get_sessid', methods=['GET'])
def get_sessid():
    login_url = f'https://v.topevery.com/StandardApiAction_login.action?account=CYJDHYHW&password=CY@jdhw1024'
    try:
        response = requests.get(login_url)
        data = response.json()
        if data.get('result') == 0 and 'jsession' in data:
            sessid = data['jsession']
            return jsonify({'sessid': sessid})
        else:
            return jsonify({'error': 'Failed to get sessid', 'details': data}), 500
    except Exception as e:
        return jsonify({'error': 'Exception occurred', 'details': str(e)}), 500


if __name__ == '__main__':
    # 启动数据库更新器（跟踪器）线程
    tracker_thread = threading.Thread(target=start_all_trackers)
    tracker_thread.start()

    # 启动异步任务：启动 SessionManager
    async_task_thread = threading.Thread(target=start_async_task)
    async_task_thread.start()

    # 启动 API 服务
    app.run(debug=False, host='0.0.0.0', port=8011)
