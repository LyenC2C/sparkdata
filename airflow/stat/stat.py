from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

import os

default_args = {
    'owner': 'lyen',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 19,10,30),
    'email': ['airflow_airflow@163.com'],
    'email_on_failure': True,
}

dag = DAG('stat', default_args=default_args, schedule_interval="@once")

xianyu_iteminfo_cmd = "(bash xianyu_iteminfo_stat.sh)"

def get_xianyu_iteminfo():
    try:
        result = os.popen(xianyu_iteminfo_cmd, "r").readline()
    except:
        raise Exception("operation failed!")
    else:
        return result


task_bash = BashOperator(task_id='xianyu_iteminfo_stat',
                         bash_command=xianyu_iteminfo_cmd,
                         dag=dag)


import re
arr_v = re.findall("(\d+)",s)[8:-1]
arr_key = ["update_day","total_files","total_rows","total_size"]
stat = dict(zip(arr_key,arr_v))
print stat
for k,v in stat.iteritems():
    print k,v