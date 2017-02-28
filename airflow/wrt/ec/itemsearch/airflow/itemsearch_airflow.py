from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.utils.helpers import chain
import sys
import os

reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'wrt',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 8),
    'email': ['airflow_airflow@163.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('itemsearch', default_args=default_args, schedule_interval=None)
sshHook = SSHHook(conn_id="cs220_wrt")

path = Variable.get('cs220_itemsearch')
check_partition_cmd = "ssh -p 22 wrt@cs220 bash {path}/get_latest_partition.sh".format(path=path)


def get_lastday():
    import datetime
    today = datetime.datetime.now()
    lastday = (today + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    return lastday


def get_last_update_date():
    try:
        result = os.popen(check_partition_cmd, "r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return str(eval(result))


spark = SSHExecuteOperator(
    task_id="itemsearch_spark",
    bash_command='(bash {path}/itemsearch_parse.sh {lastday})'.format(path=path, lastday=get_lastday()),
    ssh_hook=sshHook,
    dag=dag)

hive_distinct = SSHExecuteOperator(
    task_id="itemsearch_hive_dis",
    bash_command='(bash {path}/distinct.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

hive = SSHExecuteOperator(
    task_id="itemsearch_hive_import",
    bash_command='(bash {path}/itemsearch_import.sh {lastday} {last_update_day})'.format(path=path,
                                                                                         lastday=get_lastday(),
                                                                                         last_update_day=get_last_update_date()),
    ssh_hook=sshHook,
    dag=dag)

final_ops = SSHExecuteOperator(
    task_id="hdfs_ops",
    bash_command='(bash {path}/archive.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

email = EmailOperator(task_id='itemsearch_email',
                      to=['airflow_airflow@163.com'],
                      subject='itemsearch workflow',
                      html_content='[ itemsearch updated!!!!! ]',
                      dag=dag)

chain(spark, hive_distinct, hive, final_ops, email)
