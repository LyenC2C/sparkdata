from airflow import DAG
from airflow.utils.helpers import chain
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

from datetime import datetime,timedelta
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017,1,23,4,50),
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
path = Variable.get('cs220_ec_tb')


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

email = EmailOperator(task_id='shopitem_b_email',
                      to=['lienlian@wolongdata.com','wangruitong@wolongdata.com'],
                      subject='ec shopitem b workflow',
                      html_content='[ ec shopitem b data updated!!! ]',
                      dag=dag)

chain(spark,hive,email)