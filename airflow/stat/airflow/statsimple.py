from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator

import sys,json, re

reload(sys)
sys.setdefaultencoding("utf-8")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2017, 2, 28, 3, 20),
    "email": ["airflow_airflow@163.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("stat", default_args=default_args, schedule_interval="0 8 * * *")

file = "/home/lel/stat/stat.log"

stat = BashOperator(
    task_id="agg_stat",
    bash_command="(bash /home/lel/sparkdata/airflow/stat/stat.sh)",
    dag=dag)

def valid_jsontxt(content):
    if type(content) == type(u""):
        res = content.encode("utf-8")
    else:
        res = str(content)
    return res.replace('\n', "").replace("\r", "").replace('\001', "").replace("\u0001", "")


def process(jsonStr):
    ob = json.loads(valid_jsontxt(jsonStr))
    table = ob.get("table")

    last_day = ob.get("update_day")
    update_day = "latest_update_time:" + last_day.get("update_day")
    total_files = "files:" + last_day.get("total_files")
    total_size = "size:" + last_day.get("total_size")
    total_rows = last_day.get("total_rows")
    rows = "rows:" + re.findall("\\d+", total_rows)[0]

    last_2_days = ob.get("last_update_day")
    last_update_day = "last_update_time:  " + last_2_days.get("last_update_day")
    last_total_files = "files:" + last_2_days.get("total_files")
    last_total_size = "size:" + last_2_days.get("total_size")
    last_total_rows = last_2_days.get("total_rows")
    last_rows = "rows:" + re.findall("\\d+", last_total_rows)[0]
    res = "table:" + "\t" + table + "\n" + "\t".join(
        [last_update_day, last_rows, last_total_files, last_total_size]) + "\n" + "\t".join(
        [update_day, rows, total_files, total_size])
    return res

def get_lines(file):
    lines = []
    with open(file, "r") as f:
        lines = f.readlines()
    return lines[-8::]

def formatOutput(arr):
    return "\n------------------------------------------------------\n".join(arr)


data = map(process, get_lines(file))
email = EmailOperator(task_id='show',
                      to=['airflow_airflow@163.com'],
                      subject='<!--数据更新概览!-->',
                      html_content='<!--数据更新概览!-->\n{data}'.format(data=formatOutput(data)),
                      dag=dag)
