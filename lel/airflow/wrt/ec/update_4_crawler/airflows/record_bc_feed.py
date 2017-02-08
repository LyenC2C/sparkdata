from airflow import DAG
from airflow.utils.helpers import chain
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime, timedelta
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 7, 4, 55),
    'email': ['lienlian@wolongdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'end_date': datetime(2016, 12, 21),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG('record_bc_feed', default_args=default_args, schedule_interval='0 5 * * *')
sshHook = SSHHook(conn_id="cs220_wrt")
path = Variable.get('cs220_update_4_crawler')

check_partition_b_cmd = "ssh -p 22 wrt@cs220 bash {path}/get_latest_b_partition.sh".format(path=path)
check_partition_c_cmd = "ssh -p 22 wrt@cs220 bash {path}/get_latest_c_partition.sh".format(path=path)

def get_last_update_b_date():
    try:
        result = os.popen(check_partition_b_cmd, "r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return str(eval(result))

def get_last_update_b_date():
    try:
        result = os.popen(check_partition_c_cmd, "r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return str(eval(result))



# b = SSHExecuteOperator(
#     task_id="update_b",
#     bash_command='(bash {path}/record_b_feed.sh {latest_partition})'.format(path=path,
#                                                                             latest_partition=get_last_update_date()[0]),
#     ssh_hook=sshHook,
#     dag=dag)
#
# c = SSHExecuteOperator(
#     task_id="update_c",
#     bash_command='(bash {path}/record_b_feed.sh {latest_partition})'.format(path=path, lastday=get_last_update_date()[1]),
#     ssh_hook=sshHook,
#     dag=dag)
#
# chain(b, c)
