# coding=utf-8
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from datetime import datetime, timedelta

import os
import json
import re

# default_args = {
#     'owner': 'lyen',
#     'depends_on_past': False,
#     'start_date': datetime(2016, 12, 19,10,30),
#     'email': ['airflow_airflow@163.com'],
#     'email_on_failure': True,
# }
#
# dag = DAG('stat', default_args=default_args, schedule_interval="@once")
#
# xianyu_iteminfo_cmd = "(bash /home/lel/sparkdata/airflow/stat/xianyu_iteminfo_stat.sh)"
xianyu_itemcomment_cmd = "(bash /home/lel/sparkdata/airflow/stat/xianyu_itemcomment_stat.sh)"

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")

def get_from_shell(cmd):
    try:
        result = os.popen(cmd, "r").readline()
    except:
        raise Exception("operation failed!")
    else:
        return result
'''
def parse_str(content):
    arr_v = re.findall("(\d+)",content)[8:-1]
    arr_key = ["update_day","total_files","total_rows","total_size"]
    return dict(zip(arr_key,arr_v))
'''

def process(jsonStr):
    ob = json.loads(valid_jsontxt(jsonStr))
    table = ob.get("table")

    last_day = ob.get("update_day")
    update_day = "update_time:"+"\t "+last_day.get("update_day")
    total_files = "fiels:"+last_day.get("total_files")
    total_rows = last_day.get("total_rows")
    rows = "rows:"+re.findall("\\d+",total_rows)[0]

    last_2_days = ob.get("last_update_day")
    last_update_day = "last_update_time:"+last_2_days.get("last_update_day")
    last_total_files = "fiels:"+last_2_days.get("total_files")
    last_total_rows = last_2_days.get("total_rows")
    last_rows = "rows:"+re.findall("\\d+",last_total_rows)[0]
def processMulti(arr):
    result = []
    [result.append(process(table)) for table in arr]
    return result

def formatOutput(arr):
    return "\n------------------------------------------------------\n".join(arr)

print formatOutput(processMulti(process(get_from_shell(xianyu_itemcomment_cmd))))



