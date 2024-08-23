import pandas as pd
from sqlalchemy import create_engine

# 读取Excel文件
file_path = '华邺新城区项目车辆信息419.xlsx'
df = pd.read_excel(file_path)

# 重命名DataFrame中的列，使其与数据库表的字段名匹配
df.rename(columns={
    '编号': 'id',
    '车牌号码': 'license_plate',
    '发票车辆类型': 'vehicle_type',
    '品牌型号': 'brand_model',
    '车辆识别号': 'vehicle_identification_number',
    '发动机号': 'engine_number',
    '总质量': 'gross_weight',
    '所有人': 'owner',
    '备注': 'remarks',
    '名称': 'vehicle_name',
    '设备ID（carId）': 'carId',
    '车辆组': 'vehicle_group',
    '项目类别': 'project_category',
    '终端型号': 'terminal_model',
    '终端号': 'terminal_number'
}, inplace=True)

# 删除 DataFrame 中不存在于数据库表中的列
df.drop(columns=['行驶证注册时间', '行驶证有效期'], errors='ignore', inplace=True)

# 筛选出项目类别为 "渣土项目" 的数据
filtered_df = df[df['project_category'] == '渣土项目']

# 数据库连接信息
host = '111.173.89.238'
username = 'yjj'
password = 'pass'
port = 3306
database = 'CARS'  # 数据库名称

# 创建数据库连接
engine = create_engine(f"mysql+mysqlconnector://{username}:{password}@{host}:{port}/{database}")

# 将筛选后的数据导入到 VehicleInfo 表
filtered_df.to_sql('vehicleinfo', con=engine, if_exists='append', index=False)

print("渣土项目数据导入成功！")
