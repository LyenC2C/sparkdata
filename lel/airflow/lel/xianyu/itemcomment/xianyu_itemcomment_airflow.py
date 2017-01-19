from airflow import DAG
from airflow.utils.helpers import chain
from datetime import datetime,timedelta
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models import Variable

import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017,1,20,7,20),
    'email': ['lienlian@wolongdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'end_date': datetime(2016, 12, 21),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG('xianyu_itemcomment',default_args=default_args,schedule_interval='30 7 * * *')
sshHook = SSHHook(conn_id="cs220")
path = Variable.get('lel_xianyu_itemcomment')


def get_lastday():
    import datetime
    today = datetime.datetime.now()
    lastday = (today + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    return lastday

check_dir_cmd = "ssh -p 22 lel@cs220 bash {path}/check.sh {date}".format(date=get_lastday(),path=path)
check_partition_cmd ="ssh -p 22 lel@cs220 bash {path}/get_partition.sh".format(path=path)

def check_attach():
    try:
        result = os.popen(check_dir_cmd,"r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return  '1' if '1' in result else '0'

def get_last_update_date():
    try:
        result = os.popen(check_partition_cmd).readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return  str(eval(result))

spark = SSHExecuteOperator(
    task_id="comment_parse",
    bash_command='(bash {path}/xianyu_itemcomment_parse.sh {lastday})'.format(path=path,lastday=get_lastday()),
    ssh_hook=sshHook,
    dag=dag)

hive = SSHExecuteOperator(
    task_id="comment_import",
    bash_command='(bash {path}/xianyu_itemcomment_import.sh {lastday} {last_update_date})'.format(path=path,lastday=get_lastday(),last_update_date=get_last_update_date()),
    ssh_hook=sshHook,
    dag=dag)

email_update = EmailOperator(task_id='xianyu_itemcomment_update_email',
                      to=['lienlian@wolongdata.com'],
                      subject='xianyu itemcomment workflow',
                      html_content='[ xianyu data updated!!! ]',
                      dag=dag)
email_update_not = EmailOperator(task_id='xianyu_itemcomment_update_not_email',
                             to=['lienlian@wolongdata.com'],
                             subject='xianyu itemcomment workflow',
                             html_content='[ xianyu data updating!!! ]',
                             dag=dag)
branching = BranchPythonOperator(task_id='check_attach',
                                              python_callable=lambda: check_attach(),
                                              dag=dag)
chain(branching,spark,hive,email_update)
chain(branching,email_update_not)