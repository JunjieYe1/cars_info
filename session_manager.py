import os
import json
import requests
from datetime import datetime, timedelta
import pytz


class SessionManager:
    def __init__(self, file_path="session_data.json"):
        self.session_url = 'http://220.178.1.18:8542/GPSBaseserver/shiro/login.do'
        self.username = 'ahhygs'
        self.password = "123456"
        self.file_path = file_path

    def get_session_id(self):
        """从本地文件获取 session_id"""
        # 检查文件是否存在
        if os.path.exists(self.file_path):
            with open(self.file_path, 'r') as f:
                data = json.load(f)
                session_id = data.get('session_id')
                updated_at = datetime.strptime(data.get('updated_at'), '%Y-%m-%d %H:%M:%S')

                # 检查 session 是否过期，假设超过 10 小时即过期
                if datetime.now() - updated_at > timedelta(hours=10):
                    print("session_id 已过期，刷新 session_id")
                    session_id = self.refresh_session_id()
                    if session_id:
                        self.update_session_id(session_id)
                return session_id
        else:
            print("文件中没有 session_id，获取新的 session_id")
            session_id = self.refresh_session_id()
            if session_id:
                self.update_session_id(session_id)
            return session_id

    def refresh_session_id(self):
        """获取新的 session_id，通常是通过登录请求"""
        try:
            headers = {'Content-Type': 'application/json'}
            payload = {'userName': self.username, 'userPass': self.password}
            response = requests.post(self.session_url, json=payload, headers=headers)

            if response.status_code == 200:
                data = response.json()
                data = data['data']
                if 'sessionId' in data:
                    return data['sessionId']
                else:
                    raise Exception("Failed to get session_id")
            else:
                raise Exception(f"HTTP 请求失败，状态码: {response.status_code}")
        except Exception as e:
            print(f"刷新 session_id 时出错: {e}")
            return None

    def update_session_id(self, session_id):
        """将新的 session_id 更新到本地文件"""
        try:
            if session_id:
                # 获取当前北京时间
                cst = pytz.timezone('Asia/Shanghai')
                now = datetime.now(cst)

                # 将 session_id 和更新时间保存到文件
                data = {
                    "session_id": session_id,
                    "updated_at": now.strftime('%Y-%m-%d %H:%M:%S')
                }

                with open(self.file_path, 'w') as f:
                    json.dump(data, f)
                print(f"session_id 更新成功: {session_id}，更新时间: {now}")
            else:
                print("无有效 session_id，跳过更新")
        except Exception as e:
            print(f"更新 session_id 时出错: {e}")


# 用法示例：
if __name__ == "__main__":
    session_manager = SessionManager()
    session_id = session_manager.get_session_id()  # 获取或刷新 session_id
    print(f"当前 session_id: {session_id}")
