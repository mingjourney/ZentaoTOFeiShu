import time
from tools import update_table_reminder

# 查询时间间隔（秒）
QUERY_INTERVAL = 60

while True:
    update_table_reminder()
    time.sleep(QUERY_INTERVAL)