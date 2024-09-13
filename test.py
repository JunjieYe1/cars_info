import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, insert, update
from sqlalchemy.orm import sessionmaker

# 读取车辆2.csv文件
file_path2 = '车辆2.csv'
df2 = pd.read_csv(file_path2, encoding='gbk')

# 提取车牌号码和车辆组
license_group_pairs = df2[['车牌号码', '车辆组']].to_dict('records')

# 创建数据库连接（根据你的数据库配置进行调整）
db_url = 'mysql+pymysql://yjj:pass@111.173.89.238/cars'  # 替换为你的数据库连接信息
engine = create_engine(db_url)

# 创建一个Session
Session = sessionmaker(bind=engine)
session = Session()

# 反射获取 vehicleinfo 表
metadata = MetaData()
vehicleinfo = Table('vehicleinfo', metadata, autoload_with=engine)

# 遍历车辆2表中的车牌和车辆组信息
for record in license_group_pairs:
    license_plate = record['车牌号码']
    vehicle_group = record['车辆组']

    # 查找相同车牌的记录
    stmt = vehicleinfo.select().where(vehicleinfo.c.license_plate == license_plate)
    results = session.execute(stmt).fetchall()

    if results:
        # 如果存在记录且vehicle_group为空，则更新vehicle_group
        if results[0].vehicle_group is None:
            update_stmt = (
                vehicleinfo.update()
                .where(vehicleinfo.c.license_plate == license_plate)
                .values(vehicle_group=vehicle_group)
            )
            session.execute(update_stmt)
            print(f"Updated vehicle_group for license_plate: {license_plate}")
    else:
        # 如果没有记录，插入新记录
        insert_stmt = (
            vehicleinfo.insert()
            .values(license_plate=license_plate, vehicle_group=vehicle_group, project_category='渣土项目')
        )
        session.execute(insert_stmt)
        print(f"Inserted new record for license_plate: {license_plate}")

# 提交事务
session.commit()

# 关闭会话
session.close()


# SELECT user, host FROM mysql.user;
# UPDATE mysql.user SET host = '115.198.61.254' WHERE user = 'yjj' AND host = '125.121.179.157'; FLUSH PRIVILEGES;SELECT user, host FROM mysql.user;