from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016,12,30),
    'email': ['lienlian@wolongdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    #'end_date': datetime(2016, 12, 21),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,

}

dag = DAG('xianyu_item_info',default_args=default_args,schedule_interval='30 18 * * *')
sshHook = SSHHook(conn_id="cs220")

spark = SSHExecuteOperator(
    task_id="info_spark",
    bash_command='(bash /home/lel/wolong/sparkdata/lel/airflow/xianyu/source/xianyu_iteminfo_parse.sh)',
    ssh_hook=sshHook,
    dag=dag)

hive = SSHExecuteOperator(
    task_id="info_hive",
    bash_command='(bash /home/lel/wolong/sparkdata/lel/airflow/xianyu/source/xianyu_iteminfo_import.sh)',
    ssh_hook=sshHook,
    dag=dag)

email = EmailOperator(task_id='xianyu_iteminfo_email',
                      to=['lienlian@wolongdata.com'],
                      subject='xianyu iteminfo workflow',
                      html_content='successfully execute the jobs!!!!',
                      dag=dag)

spark.set_downstream(hive)
hive.set_downstream(email)