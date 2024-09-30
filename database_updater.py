# run_all_trackers.py

import threading
from vehicle_tracker import VehicleTracker
from history_info import DailyDataTracker
import logging
from config import LOG_FILE_TRACKER, LOG_FILE_DAILY

def start_vehicle_tracker():
    """启动 VehicleTracker"""
    tracker = VehicleTracker(loop_interval=5)  # 设定循环间隔，例如每5分钟执行一次
    tracker.start()

def start_daily_data_tracker():
    """启动 DailyDataTracker"""
    daily_tracker = DailyDataTracker(loop_interval=5)  # 设定循环间隔，例如每60分钟执行一次
    daily_tracker.start()

def main():
    # 配置主日志（可选）
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    logger.info("启动所有跟踪器...")

    # 创建线程
    vehicle_tracker_thread = threading.Thread(target=start_vehicle_tracker, name='VehicleTrackerThread')
    daily_data_tracker_thread = threading.Thread(target=start_daily_data_tracker, name='DailyDataTrackerThread')

    # 启动线程
    vehicle_tracker_thread.start()
    daily_data_tracker_thread.start()

    logger.info("所有跟踪器已启动。")

    # 等待线程完成
    try:
        vehicle_tracker_thread.join()
        daily_data_tracker_thread.join()
    except KeyboardInterrupt:
        logger.info("程序终止。")

if __name__ == "__main__":
    main()
