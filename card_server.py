from flask import Flask, request, jsonify  
import pymysql

# 数据库连接配置
DB_HOST = "47.98.228.23"
DB_USER = "root"
DB_PASSWORD = "Tpjxc2023!"
DB_PORT = 15017
DB_NAME = "zentao"

app = Flask(__name__)  
def get_db_connection():
    db = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, port=DB_PORT, db=DB_NAME, charset='utf8')
    return db

def close_db_connection(db):
    db.close()
def update_task_status(db,id):
    sql = "UPDATE zt_task SET status = %s WHERE id = %s"  
    cursor = db.cursor()  
    new_status = 'doing'  
    result = cursor.execute(sql, (new_status, id)) 
    db.commit()  
    cursor.close()
    return result  
@app.route('/card_subscription', methods=['POST'])  
def event_subscription():  

    json_data = request.get_json()  
    print(json_data)
    print(json_data["action"])

    id = json_data['action']['value']['id']
    db = get_db_connection()
    result = update_task_status(db,id)
    print(result) 

    card_content = "回调后卡片内容"  
    response = {'card': card_content}  
    return jsonify(response)
  
if __name__ == '__main__':  
    app.run(host='0.0.0.0',port=5000)