from airflow import DAG
from datetime import datetime,timedelta
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016,12,30),
    #    'email': ['lienlian@wolongdata.com'],
    #    'email_on_failure': True,
    #    'email_on_retry': True,
    #    'retries': 3,
    #    'retry_delay': timedelta(minutes=5),
    #'end_date': datetime(2016, 12, 21),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,

}

dag = DAG('cs220_test',default_args=default_args,schedule_interval='*/1 16 * * *')
sshHook = SSHHook(conn_id="cs220")

bash_simple = SSHExecuteOperator(
    task_id="simple",
    bash_command='date',
    ssh_hook=sshHook,
    env={"SHELL":"/bin/bash"},
    dag=dag)

