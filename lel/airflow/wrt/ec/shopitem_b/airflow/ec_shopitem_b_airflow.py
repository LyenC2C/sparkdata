from airflow import DAG
from airflow.utils.helpers import chain
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime,timedelta
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017,2,7,4,55),
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

dag = DAG('shopitem_b',default_args=default_args,schedule_interval='0 5 * * *')
sshHook = SSHHook(conn_id="cs220_wrt")
path = Variable.get('cs220_ec_shopitem_b')

def get_lastday():
    import datetime
    today = datetime.datetime.now()
    lastday = (today + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    return lastday

check_dir_cmd = "ssh -p 22 wrt@cs220 bash {path}/check.sh {date}".format(date=get_lastday(),path=path)
check_partition_cmd ="ssh -p 22 wrt@cs220 bash {path}/get_latest_partition.sh".format(path=path)

def check_attach():
    try:
        result = os.popen(check_dir_cmd,"r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return  'update' if '1' in result else 'pass'

def get_last_update_date():
    try:
        result = os.popen(check_partition_cmd,"r").readline()
    except:
        raise Exception("ssh operation failed!")
    else:
        return  str(eval(result))



spark = SSHExecuteOperator(
    task_id="shopitem_b_parse",
    bash_command='(bash {path}/ec_itemsold_sale_parse.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

hive = SSHExecuteOperator(
    task_id="shopitem_b_import",
    bash_command='(bash {path}/ec_itemsold_sale_import.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

email_update = EmailOperator(task_id='shopitem_b_updated_email',
                             to=['lienlian@wolongdata.com'],
                             subject='ec shopitem b workflow',
                             html_content='[ ec shopitem b data updated!!! ]',
                             dag=dag)

email_update_not = EmailOperator(task_id='shopitem_b_not_update_email',
                                 to=['lienlian@wolongdata.com'],
                                 subject='ec shopitem b workflow',
                                 html_content='[ ec shopitem b data updated!!! ]',
                                 dag=dag)

branching = BranchPythonOperator(task_id='check_attach',
                                 python_callable=lambda: check_attach(),
                                 dag=dag)

passover = DummyOperator(task_id='pass', dag=dag)
update = DummyOperator(task_id='update', dag=dag)

chain(branching,passover,email_update_not)
chain(branching,update,spark,hive,email_update)

