from airflow import DAG
from airflow.utils.helpers import chain
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 8),
    'email': ['airflow_airflow@163.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('record_bc_feed', default_args=default_args, schedule_interval='None')
sshHook = SSHHook(conn_id="cs220_wrt")
path = Variable.get('cs220_update_4_crawler')

check_partition_b_cmd = "ssh -p 22 wrt@cs220 bash {path}/record_bc_feed/get_latest_b_partition.sh".format(path=path)
check_partition_c_cmd = "ssh -p 22 wrt@cs220 bash {path}/record_bc_feed/get_latest_c_partition.sh".format(path=path)


def get_last_update_b_date():
    try:
        result = os.popen(check_partition_b_cmd, "r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return str(eval(result))


def get_last_update_c_date():
    try:
        result = os.popen(check_partition_c_cmd, "r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return str(eval(result))


b = SSHExecuteOperator(
    task_id="update_b",
    bash_command='(bash {path}/record_bc_feed/record_b_feed.sh {latest_partition})'.format(path=path,
                                                                            latest_partition=get_last_update_b_date()),
    ssh_hook=sshHook,
    dag=dag)

c = SSHExecuteOperator(
    task_id="update_c",
    bash_command='(bash {path}/record_bc_feed/record_c_feed.sh {latest_partition})'.format(path=path,
                                                                            latest_partition=get_last_update_c_date()),
    ssh_hook=sshHook,
    dag=dag)

chain(b, c)
