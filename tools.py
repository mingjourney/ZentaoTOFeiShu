import json
import datetime
import pymysql
import requests
import time
from enum import Enum
from datetime import date

# Feishu配置
FEISHU_APP_ID = "cli_a424e7a645f9500b"
FEISHU_APP_SECRET = "yH9jUcSJwQFtjuQmaSWHVg2jDtqDcGYJ"
# 数据库连接配置
DB_HOST = "47.98.228.23"
DB_USER = "root"
DB_PASSWORD = "Tpjxc2023!"
DB_PORT = 15017
DB_NAME = "zentao"
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)
def save_record_ids(record_ids):
    with open("record_ids.json", "w") as file:
        json.dump(record_ids, file)
def save_result(result):
    converted_result = [[str(field) if field is not None else None for field in record] for record in result]
    with open("results.json", "w") as file:
        json.dump(converted_result, file, indent=4, cls=CustomJSONEncoder)

def load_result():
    try:
        with open("results.json", "r") as file:
            record_ids = json.load(file)
            return record_ids
    except FileNotFoundError:
        return []
def load_record_ids():
    try:
        with open("record_ids.json", "r") as file:
            record_ids = json.load(file)
            return record_ids
    except FileNotFoundError:
        return []

def get_db_connection():
    # 创建数据库连接
    db = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT, db=DB_NAME, charset='utf8')
    return db

def close_db_connection(db):
    # 关闭数据库连接
    db.close()

def get_tenant_access_token():
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    data = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_SECRET
    }
    response = requests.post(url=token_url, headers=headers, json=data)
    tenant_access_token = json.loads(response.content)['tenant_access_token']
    return tenant_access_token

def query_task_data(db):
    cursor = db.cursor()
    sql = "SELECT b.id, b.name, b.pri, u.realname, u.mobile, u1.realname AS openName, b.type, b.deleted, b.status, b.deadline \
           FROM zt_task b \
           LEFT JOIN zt_user u ON b.assignedTo = u.account \
           LEFT JOIN zt_user u1 ON b.openedBy = u1.account"
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    return result

class TaskStatus(Enum):
    doing = '进行中'
    done = '已完成'
    waiting = '未开始'

def generate_task_messages(result, user_ids):
    # 生成消息内容列表
    messages_list = []
    for row, user_id in zip(result, user_ids):
        id = row[0]
        title = row[1]
        pri = row[2]
        realname = row[3]
        moblie = row[4]
        openname = row[5]
        types = row[6]
        status = row[8]
        deadline = row[9]
        status_dict = {
            "doing": "进行中",
            "wait": "未开始",
            "done": "已完成"
        }
        pri_dict = {
            1: "p1",
            2: "p2",
            3: "p3"
        }
        fields = {
            "任务": title,
            "优先级": pri_dict.get(pri, "未知优先级"),
            "状态": status_dict.get(status, "已关闭"),
            "🧑🏻‍💻  当前指派给": [
                {"id": str(user_id)}
            ],
        }
        if deadline != "0000-00-00" and deadline is not None:
            try:
                deadline_str = str(deadline)
                deadline_dt = datetime.datetime.strptime(deadline_str, "%Y-%m-%d")
                deadline_timestamp = int(deadline_dt.timestamp()) * 1000
                fields["截止日期"] = deadline_timestamp
            except ValueError:
                pass
        messages_list.append(fields)
    return messages_list

def get_user_ids(tenant_access_token, mobile_list):
    # 将手机号列表分成多个批次，每个批次最多包含50个手机号
    batch_size = 50
    mobile_batches = [mobile_list[i:i+batch_size] for i in range(0, len(mobile_list), batch_size)]
    
    user_ids = []

    # 发起批量请求，查询每个批次中手机号对应的用户ID
    for mobile_batch in mobile_batches:
        # 获取用户的 user_id
        url = "https://open.feishu.cn/open-apis/contact/v3/users/batch_get_id?user_id_type=open_id"
        headers = {
            'Content-Type': 'application/json',
            "Authorization": "Bearer %s" % tenant_access_token,
        }
        data = {
            "mobiles": mobile_batch
        }
        response = requests.post(url=url, headers=headers, json=data)
        user_list = json.loads(response.content)["data"]["user_list"]
        user_ids.extend([user.get('user_id', 'ou_3efa242e61abfa7e568e7e499e319c8f') for user in user_list])

    return user_ids

def update_table(user_ids, messages_list, tenant_access_token):
    # 更新多维表
    app_token = "HUcqb1kldaKpJOsm4JVcVdrXnNd"
    table_id = "tblRTE7HmEp1DWG5"
    delete_url = "https://open.feishu.cn/open-apis/bitable/v1/apps/" + app_token + "/tables/" + table_id + "/records/batch_delete"
    send_url = "https://open.feishu.cn/open-apis/bitable/v1/apps/" + app_token + "/tables/" + table_id + "/records/batch_create"
    headers = {
        "Authorization": "Bearer %s" % tenant_access_token,
        "Content-Type": "application/json"
    }
    payloads = {
        "records": load_record_ids()
    }
    response = requests.post(url=delete_url, headers=headers, json=payloads)
    print(response.text)
    record_ids = []
    payloads["records"] = []

    for message in messages_list:
        payloads["records"].append({
            "fields": message
        })
    response = requests.post(url=send_url, headers=headers, json=payloads)
    response_data = response.json()  # 解析为JSON格式
    if response_data['code'] == 0:
        records = response_data['data']['records']
        record_ids = [record['record_id'] for record in records]
        print(record_ids)
    save_record_ids(record_ids)

def is_equal_lists(list1, list2):
    # 判断两个列表是否相等
    if len(list1) != len(list2):
        print(len(list1),len(list2))
        return False
    for i in range(len(list1)):
        if list1[i] != list2[i]:
            print(list1[i],list2[i])
            return False
    return True

def update_table_reminder():
    db = get_db_connection()
    result = query_task_data(db)
    tenant_access_token = get_tenant_access_token()
    mobile_list = [row[4] if row[4] and row[4] != 'None' else '13706808013' for row in result]
    user_ids = get_user_ids(tenant_access_token, mobile_list)
    messages_list = generate_task_messages(result, user_ids)
    result_str_list = [[str(item) for item in tup] for tup in result]

    # 检查上一次记录的数据
    last_result = load_result()
    last_result_str_list = [[str(item) for item in tup] for tup in last_result]

    # print("result",result_str_list)
    # print("last_result",last_result_str_list)
    # 比较当前数据和上一次记录的数据
    if is_equal_lists(result_str_list, last_result_str_list):
        
        print("数据未发生改变，无需更新")
        close_db_connection(db)
        return

    update_table(user_ids, messages_list, tenant_access_token)

    # 更新记录的数据
    save_result(result)

    close_db_connection(db)
