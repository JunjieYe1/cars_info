from flask import Flask, jsonify, request
import aiomysql
from datetime import datetime, timedelta
from flask_cors import CORS
import aiohttp
import asyncio
from flask_caching import Cache
import json

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# 配置缓存
cache = Cache(app, config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 600})

# 数据库连接信息
db_config = {
    # 'host': '111.173.89.238',
    'host': 'localhost',
    'user': 'yjj',
    'password': 'pass',
    'db': 'Cars'
}


async def connect_db():
    return await aiomysql.connect(**db_config)


async def fetch_count_data(session, session_id, car_id, start_time, end_time):
    try:
        if car_id:
            async with session.get(
                f"http://121.37.154.193:9999/gps-web/api/get_gps_h.jsp?sessionId={session_id}&carId={car_id}&startTime={start_time}&endTime={end_time}",
                timeout=5
            ) as response:
                response.raise_for_status()
                count_data = await response.json()
                return count_data.get('countData', {})
        else:
            return {}
    except Exception as e:
        print(f"Error fetching count data for car {car_id}: {e}")
        return {}

async def fetch_sanitation_status(session, session_id):
    try:
        async with session.get(
            f"http://121.37.154.193:9999/gps-web/api/get_gps_r.jsp?sessionId={session_id}",
            timeout=5
        ) as response:
            response.raise_for_status()
            # 注意：某些情况下，响应的Content-Type可能不正确，因此指定content_type=None
            text = await response.text()
            data = json.loads(text)
            return data.get('list', [])
    except Exception as e:
        print(f"Error fetching sanitation data: {e}")
        return []

def map_status_code(alarmType):
    # 将字符串类型的alarmType转换为整数
    alarmType = int(alarmType)
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


# 添加新的函数来获取老城区环卫车辆的状态数据
async def fetch_status_data_old_urban(session, session_id):
    try:
        async with session.get(
            f"http://121.37.154.193:9999/gps-web/api/get_gps_r.jsp?sessionId={session_id}",
            timeout=5
        ) as response:
            response.raise_for_status()
            data = await response.json()
            vehicle_status_dict = {}
            if 'list' in data:
                for vehicle in data['list']:
                    carId = vehicle.get('carId')
                    alarmType = vehicle.get('state')
                    if carId and alarmType:
                        # 将报警类型编码转换为精简的状态编码
                        simplified_status = map_status_code(alarmType)
                        vehicle_status_dict[str(carId)] = simplified_status
            return vehicle_status_dict
    except Exception as e:
        print(f"Error fetching status data: {e}")
        return {}


async def fetch_count_data_zt(session, license_plate, start_time, end_time):
    try:
        if license_plate:
            async with session.get(
                f"http://111.173.89.238:7203/info_report/v1/query_track_data?vehicleNo={license_plate}&startTime={start_time}&endTime={end_time}&curPage=1&pageNum=9999",
                timeout=5
            ) as response:
                response.raise_for_status()
                data = await response.json()
                if "dataList" not in data["data"]:
                    return {}
                data_list = data["data"]["dataList"]
                if len(data_list) > 2:
                    mile = round((data_list[-1]["deviceMileage"] - data_list[0]["deviceMileage"]) / 1000, 2)
                    move_long = data_list[-1]['driveTimeLen'] if 'driveTimeLen' in data_list[-1] else 0
                    hours, remainder = divmod(move_long, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    formatted_time = ""
                    if hours > 0:
                        formatted_time += f"{hours}时"
                    if minutes > 0:
                        formatted_time += f"{minutes}分"
                    formatted_time += f"{seconds}秒"
                    move_long = formatted_time
                    return {"mile": mile, "move_long": move_long}
                else:
                    return {}
        else:
            return {}
    except Exception as e:
        print(f"Error fetching count data for car {license_plate}: {e}")
        return {}


async def fetch_count_data_new_urban(session, license_plate, start_time, end_time):
    try:
        if license_plate:
            async with session.post(
                "http://220.178.1.18:8542/GPSBaseserver/stdHisAlarm/getHisGPS.do",
                json={
                    "beginTime": start_time,
                    "endTime": end_time,
                    "vehicleNum": license_plate,
                    "sessionId": "57c7ccea-5e8c-493a-8ac8-db8598deadac-02333474"
                },
                timeout=10
            ) as response:
                response.raise_for_status()
                data = await response.json()
                if data['resultCode'] == 0 and data['data']:
                    data_list = data['data']
                    if len(data_list) > 2:
                        first_mileage = data_list[0]["mileage"]
                        last_mileage = data_list[-1]["mileage"]
                        mile = round((last_mileage - first_mileage) / 1000, 1)
                        return {"mile": mile}
                return {"mile": 0.0}
        else:
            return {"mile": 0.0}
    except Exception as e:
        print(f"Error fetching data for car {license_plate}: {e}")
        return {"mile": 0.0}


async def fetch_status_data_new_urban(session, session_id):
    try:
        async with session.post(
            "http://220.178.1.18:8542/GPSBaseserver/stdHisAlarm/getGPS.do",
            json={
                "sessionId": session_id
            },
            timeout=10
        ) as response:
            response.raise_for_status()
            data = await response.json()
            vehicle_status_dict = {}
            if data['resultCode'] == 0 and 'data' in data:
                for vehicle in data['data']:
                    vehicleNum = vehicle.get('vehicleNum')  # 车牌号
                    gpsTimeStr = vehicle.get('gpsTime')     # GPS时间
                    stateStr = vehicle.get('stateStr')      # 状态字符串
                    speed = vehicle.get('speed', 0)         # 速度，默认为0
                    gpsTime = datetime.strptime(gpsTimeStr, '%Y-%m-%d %H:%M:%S') if gpsTimeStr else None

                    # 判断是否在线
                    if gpsTime and gpsTime.date() == datetime.now().date():
                        # 在线，解析状态
                        status = parse_new_urban_status(stateStr, speed)
                    else:
                        status = 0  # 离线

                    # 保存状态
                    if vehicleNum:
                        vehicle_status_dict[vehicleNum] = status
            return vehicle_status_dict
    except Exception as e:
        print(f"Error fetching new urban status data: {e}")
        return {}


def parse_new_urban_status(stateStr, speed):
    # 默认状态为未知
    status = -1
    if not stateStr:
        return status

    # 提取第一个状态
    first_state = stateStr.split(',')[0]

    if first_state == "车辆点火":
        if speed > 0:
            status = 2  # 行驶
        else:
            status = 1  # 停车
    elif first_state == "车辆熄火":
        status = 3  # 熄火
    else:
        status = -1  # 未知状态

    return status


def make_cache_key():
    date_str = request.args.get('date', 'default')
    return f"{request.path}?date={date_str}"


@app.route('/api/last_locations', methods=['GET'])
@cache.cached(timeout=600, key_prefix=make_cache_key)
async def get_last_locations():
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({"error": "Date parameter is required"}), 400

    try:
        date = datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400

    connection = await connect_db()
    cursor = await connection.cursor()

    # 查询所有车辆信息
    await cursor.execute("""
        SELECT 
            id, license_plate, carId, vehicle_group, project_category, terminal_model, terminal_number,
            brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type, driver
        FROM VehicleInfo
    """)
    vehicles = await cursor.fetchall()

    # 定义session_id
    session_id = "sNRkJpZXYwF2dmdmdmgCQmNlYIN3S1Nnawhic3kWNPNjZ5JWYeFWepZCZxpnch9VYf1mYmVmdkFXby9FRfNFNvJDawEXeY"
        # 新城区项目的session_id
    session_id_new_urban = "57c7ccea-5e8c-493a-8ac8-db8598deadac-02333474"

    # 使用aiohttp的ClientSession
    async with aiohttp.ClientSession() as session:
        # 获取老城区环卫车辆的状态数据
        status_data_old_urban = await fetch_status_data_old_urban(session, session_id)
        # 获取新城区项目车辆的状态数据
        status_data_new_urban = await fetch_status_data_new_urban(session, session_id_new_urban)

        status_data = {**status_data_old_urban, **status_data_new_urban}
        # 定义一个异步任务来并发处理车辆数据
        tasks = [handle_vehicle_data(vehicle, session_id, date, session, status_data) for vehicle in vehicles]

        # 并发执行所有车辆数据的获取任务
        results = await asyncio.gather(*tasks)

    await cursor.close()
    connection.close()

    return jsonify(results)


# 处理单个车辆数据的异步函数
async def handle_vehicle_data(vehicle, session_id, date, session, status_data):
    vehicle_id, license_plate, car_id, vehicle_group, project_category, terminal_model, terminal_number, \
        brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type, driver = vehicle

    # 查询当天是否有轨迹信息
    track = await fetch_track_info(vehicle_id, date)

    # 初始化状态信息
    status, latitude, longitude, last_time = process_track_info(track)

    move_long = 'N/A'
    mile = 'N/A'

    # 根据不同项目类别获取对应的轨迹数据
    if car_id:
        count_data = await fetch_count_data(session, session_id, car_id, date.strftime('%Y%m%d000000'),
                                            date.strftime('%Y%m%d235959'))
        move_long = count_data.get('move_long', 'N/A')
        mile = count_data.get('mile', 'N/A')
    elif project_category == "渣土项目":
        count_data = await fetch_count_data_zt(session, license_plate, date.strftime('%Y-%m-%d 00:00:00'),
                                               date.strftime('%Y-%m-%d 23:59:59'))
        move_long = count_data.get('move_long', 'N/A')
        mile = count_data.get('mile', 'N/A')
    elif project_category == "新城区项目":
        count_data = await fetch_count_data_new_urban(session, license_plate, date.strftime('%Y-%m-%d 00:00:00'),
                                                      date.strftime('%Y-%m-%d 23:59:59'))
        mile = count_data.get('mile', 'N/A')

    # 更新老城区环卫车辆的状态信息
    if project_category == "老城区环卫" and car_id:
        if str(car_id) in status_data:
            status = status_data[str(car_id)]
        else:
            status = -1  # 未知状态

    # 更新新城区项目车辆的状态信息
    if project_category == "新城区项目" and license_plate:
        if license_plate in status_data:
            status = status_data[license_plate]
        else:
            status = -1  # 未知状态

    return {
        'id': vehicle_id,
        'license_plate': license_plate,
        'carId': car_id,
        'vehicle_group': vehicle_group,
        'project_category': project_category,
        'terminal_model': terminal_model,
        'terminal_number': terminal_number,
        'status': status,
        'latitude': latitude,
        'longitude': longitude,
        'last_time': last_time,
        'move_long': move_long,
        'mile': mile,
        'brand_model': brand_model,
        'vehicle_identification_number': vehicle_identification_number,
        'engine_number': engine_number,
        'owner': owner,
        'vehicle_name': vehicle_name,
        'gross_weight': gross_weight,
        'vehicle_type': vehicle_type,
        'driver': driver
    }








# Fetch track info separately to avoid sharing the cursor concurrently
async def fetch_track_info(vehicle_id, date):
    connection = await connect_db()
    async with connection.cursor() as cursor:
        await cursor.execute("""
                    SELECT latitude, longitude, track_time
                    FROM VehicleTrack
                    WHERE vehicle_id = %s AND DATE(track_time) = %s
                    ORDER BY track_time DESC
                    LIMIT 1
                """, (vehicle_id, date))
        track = await cursor.fetchone()
    connection.close()  # Remove await here as connection.close() is synchronous
    return track


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

        response = [{'id': fence_id, 'name': fence['name'], 'points': fence['points']} for fence_id, fence in fences.items()]
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
                    "password": "123456",
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8011)
