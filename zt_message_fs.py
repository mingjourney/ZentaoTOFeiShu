import json
import datetime
import pymysql
import requests
import time

DB_HOST = "47.98.228.23"
DB_USER = "root"
DB_PASSWORD = "Tpjxc2023!"
DB_PORT = 15017
DB_NAME = "zentao"

# Feishu配置
FEISHU_APP_ID = "cli_a42927b5bd3ed00b"
FEISHU_APP_SECRET = "8UL4LSSanXm7Yhx118kWheEnPoHgNE1W"

# 查询时间间隔
QUERY_INTERVAL = 60

def get_db_connection():
    db = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT, db=DB_NAME, charset='utf8')
    return db

def close_db_connection(db):
    db.close()

def query_bug_data(db):
    # 获取当前时间和前一分钟的日期范围
    current_time = datetime.datetime.now()
    start_of_previous_minute = current_time - datetime.timedelta(minutes=1)
    end_of_previous_minute = current_time
    cursor = db.cursor()
    sql = "SELECT b.id, b.title, b.severity, b.pri, u.realname, u.mobile, u1.realname AS openName \
           FROM zt_bug b \
           LEFT JOIN zt_user u ON b.assignedTo = u.account \
           LEFT JOIN zt_user u1 ON b.openedBy = u1.account \
           WHERE b.openedDate BETWEEN %s AND %s"
    cursor.execute(sql, (start_of_previous_minute, end_of_previous_minute))
    result = cursor.fetchall()
    cursor.close()
    
    return result

    
def generate_bug_messages(result):
    # 生成消息内容列表
    messages_list = []
    for row in result:
        id = row[0]
        title = row[1]
        severity = row[2]
        pri = row[3]
        realname = row[4]
        moblie = row[5]
        openname = row[6]
        template_variable = {
            "title": "你有一个BUG请求待解决",
            "from": openname,
            "id": str(id),
            "priority": str(severity),
            "remark": "http://47.98.228.23:15011/bug-view-{}.html".format(id),
            "detail": title
        }
        content = {
            "type": "template",
            "data": {
                "template_id": "ctp_AA6lWV86e7uZ",
                "template_variable": template_variable
            }
        }
        
        messages_list.append(content)
    return messages_list

def get_tenant_access_token():
    # 获取企业 access_token
    token_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    data = {
        "app_id": FEISHU_APP_ID,
        "app_secret": FEISHU_APP_SECRET
    }
    response = requests.post(url=token_url, headers=headers, json=data)
    tenant_access_token = json.loads(response.content)['tenant_access_token']
    return tenant_access_token

def get_user_ids(tenant_access_token, moblie_list):
    # 获取用户的 user_id
    url = "https://open.feishu.cn/open-apis/contact/v3/users/batch_get_id?user_id_type=open_id"
    headers = {
        'Content-Type': 'application/json',
        "Authorization": "Bearer %s" % tenant_access_token,
    }
    data = {
        "mobiles": moblie_list
    }
    response = requests.post(url=url, headers=headers, json=data)
    user_list = json.loads(response.content)["data"]["user_list"]
    user_ids = [user["user_id"] for user in user_list]
    return user_ids

def send_messages(user_ids, messages_list, tenant_access_token):
    # 发送消息给用户
    send_url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id"
    headers = {
        "Authorization": "Bearer %s" % tenant_access_token,
        "Content-Type": "application/json"
    }
    for user_id, message in zip(user_ids, messages_list):  
        payload = {  
            "receive_id": user_id,  
            "msg_type": "interactive",  
            "content": json.dumps(message)  
        }  
        response = requests.post(url=send_url, headers=headers, json=payload)  
        print(response.text)

def send_bug_reminder():
    db = get_db_connection()
    result = query_bug_data(db)
    messages_list = generate_bug_messages(result)
    # 获取access_token
    tenant_access_token = get_tenant_access_token()
    # 获取user_id
    moblie_list = [row[5] for row in result]
    user_ids = get_user_ids(tenant_access_token, moblie_list)
    send_messages(user_ids, messages_list, tenant_access_token)
    close_db_connection(db)

def query_task_data(db):
    current_time = datetime.datetime.now()
    start_of_previous_minute = current_time - datetime.timedelta(minutes=1)
    end_of_previous_minute = current_time
    cursor = db.cursor()
    sql = "SELECT b.id, b.name, b.pri, u.realname, u.mobile, u1.realname AS openName, b.type\
           FROM zt_task b \
           LEFT JOIN zt_user u ON b.assignedTo = u.account \
           LEFT JOIN zt_user u1 ON b.openedBy = u1.account \
           WHERE b.openedDate BETWEEN %s AND %s"
    cursor.execute(sql, (start_of_previous_minute, end_of_previous_minute))
    result = cursor.fetchall()
    cursor.close()
    return result
def generate_task_messages(result):
    messages_list = []
    for row in result:
        id = row[0]
        title = row[1]
        pri = row[2]
        realname = row[3]
        moblie = row[4]
        openname = row[5]
        types = row[6]
        template_variable = {
            "title": "你有一个任务被指派",
            "from": openname,
            "id": str(id),
            "priority": str(pri),
            "remark": "http://47.98.228.23:15011/execution-task.html".format(id),
            "detail": title 
        }
        content = {
            "type": "template",
            "data": {
                "template_id": "ctp_AA6lWV86e7uZ",
                "template_variable": template_variable
            }
        }
        
        messages_list.append(content)
    return messages_list
def send_task_reminder():
    db = get_db_connection()
    result = query_task_data(db)
    messages_list = generate_task_messages(result)
    tenant_access_token = get_tenant_access_token()
    moblie_list = [row[4] for row in result]
    user_ids = get_user_ids(tenant_access_token, moblie_list)
    send_messages(user_ids, messages_list, tenant_access_token)
    close_db_connection(db)
while True:
    send_bug_reminder()
    send_task_reminder()
    time.sleep(QUERY_INTERVAL)