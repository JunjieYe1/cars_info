from flask import Flask, jsonify, request
import aiomysql
from datetime import datetime, timedelta
from flask_cors import CORS
import aiohttp
import asyncio
from flask_caching import Cache

app = Flask(__name__)
CORS(app)

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
        if car_id:  # 检查 carId 是否为空

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


async def fetch_count_data_zt(session, license_plate, start_time, end_time):
    try:
        if license_plate:  # 检查 license_plate 是否为空

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

                    mile = round((data_list[-1]["deviceMileage"] - data_list[0]["deviceMileage"] )/ 1000,2)
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


def make_cache_key():
    date_str = request.args.get('date', 'default')
    return f"{request.path}?date={date_str}"


@app.route('/api/last_locations', methods=['GET'])
@cache.cached(timeout=600, key_prefix=make_cache_key)  # 缓存10分钟，并根据请求参数生成缓存键
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

    # 查询所有车辆信息，包括需要的字段
    await cursor.execute("""
        SELECT 
            id, license_plate, carId, vehicle_group, project_category, terminal_model, terminal_number,
            brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type
        FROM VehicleInfo
    """)
    vehicles = await cursor.fetchall()

    response = []
    session_id = "sNRkJpZXYwF2dmdmdmgCQmNlYIN3S1Nnawhic3kWNPNjZ5JWYeFWepZCZxpnch9VYf1mYmVmdkFXby9FRfNFNvJDawEXeY"

    # 创建一个 aiohttp 客户端会话
    async with aiohttp.ClientSession() as session:
        for vehicle in vehicles:
            vehicle_id, license_plate, car_id, vehicle_group, project_category, terminal_model, terminal_number, \
                brand_model, vehicle_identification_number, engine_number, owner, vehicle_name, gross_weight, vehicle_type = vehicle

            # 查询当天是否有轨迹信息
            await cursor.execute("""
                SELECT latitude, longitude, track_time
                FROM VehicleTrack
                WHERE vehicle_id = %s AND DATE(track_time) = %s
            """, (vehicle_id, date))
            track = await cursor.fetchone()

            if track:
                status = "在线"
                latitude = track[0]
                longitude = track[1]
                last_time = track[2].strftime('%Y-%m-%d %H:%M:%S')
            else:
                status = "离线"
                latitude = None
                longitude = None
                last_time = None

            # 如果carId存在，继续获取count_data
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
            else:
                move_long = 'N/A'
                mile = 'N/A'

            response.append({
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
                'vehicle_type': vehicle_type
            })

    await cursor.close()
    connection.close()

    return jsonify(response)


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


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8011)
