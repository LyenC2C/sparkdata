from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'wrt',
    'depends_on_past': False,
    'start_date': datetime(2017,1,28,6,50),
    'email': ['lienlian@wolongdata.com','wangruitong@wolongdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'end_date': datetime(2016, 12, 21),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,

}

dag = DAG('ec_item_and_shopinfo',default_args=default_args,schedule_interval='0 7 * * 6')
sshHook = SSHHook(conn_id="cs220_wrt")

path = Variable.get('cs220_ec_item&shopinfo')

spark_item = SSHExecuteOperator(
    task_id="ec_iteminfo_spark",
    bash_command='(bash {path}/ec_iteminfo_parse.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

hive_item = SSHExecuteOperator(
    task_id="ec_iteminfo_hive",
    bash_command='(bash {path}/ec_iteminfo_import.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

spark_shop = SSHExecuteOperator(
    task_id="ec_shopinfo_spark",
    bash_command='(bash {path}/ec_shopinfo_parse.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)

hive_shop= SSHExecuteOperator(
    task_id="ec_shopinfo_hive",
    bash_command='(bash {path}/ec_shopinfo_import.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)
final_ops= SSHExecuteOperator(
    task_id="ec_hdfs_ops",
    bash_command='(bash {path}/ec_item_shop_final_ops.sh)'.format(path=path),
    ssh_hook=sshHook,
    dag=dag)
email = EmailOperator(task_id='ec_item_and_shopinfo_email',
                      to=['lienlian@wolongdata.com'],
                      subject='ec item and shop info workflow',
                      html_content='Execute the jobs successfully!!!!',
                      dag=dag)
spark_item.set_downstream(hive_item)
hive_item.set_downstream(spark_shop)
spark_shop.set_downstream(hive_shop)
hive_shop.set_downstream(final_ops)
final_ops.set_downstream(email)
