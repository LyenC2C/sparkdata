from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.hive_hooks import HiveCliHook


default_args = {
    'owner': 'lel',
    'depends_on_past': False,
    'start_date': datetime(2016,12,30),
}

dag = DAG('remote',default_args=default_args,schedule_interval='*/1 10 * * *')
sshHook = SSHHook(conn_id="cs220")
hiveCliHook = HiveCliHook(hive_cli_conn_id="cs220_HiveClient")
hiveCliHook.run_cli("select * from lel.wc_text")

remote_hive = SSHExecuteOperator(
    task_id="task1",
    bash_command='date',
    ssh_hook=sshHook,
    dag=dag)




