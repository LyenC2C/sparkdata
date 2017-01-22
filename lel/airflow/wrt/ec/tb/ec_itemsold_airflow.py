from airflow import DAG
from airflow.utils.helpers import chain
from airflow.contrib.operators.ssh_execute_operator import SSHExecuteOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

from datetime import datetime,timedelta
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017,1,23,3,50),
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

dag = DAG('itemsold_sale',default_args=default_args,schedule_interval='0 4 * * *')
sshHook = SSHHook(conn_id="cs220_wrt")
path = Variable.get('cs220_ec_tb')



def get_date():
    import datetime
    today = datetime.datetime.now()
    lastday = (today + datetime.timedelta(days=-1)).strftime('%Y%m%d')
    lastday_2_days = (today + datetime.timedelta(days=-2)).strftime('%Y%m%d')
    return (lastday,lastday_2_days)

check_partition_cmd ="ssh -p 22 wrt@cs220 bash {path}/get_partition.sh".format(path=path)


def get_last_update_date():
    try:
        result = os.popen(check_partition_cmd,"r").readline()
        print result
    except:
        raise Exception("ssh operation failed!")
    else:
        return  str(eval(result))

spark_sale = SSHExecuteOperator(
    task_id="itemsold_sale_parse",
    bash_command='(bash {path}/ec_itemsold_sale_parse.sh {last_2_days} {lastday} {last_update})'
        .format(path=path,last_2_days=get_date()[1],lastday=get_date()[0],last_update=get_last_update_date()),
    ssh_hook=sshHook,
    dag=dag)

hive_sale = SSHExecuteOperator(
    task_id="itemsold_sale_import",
    bash_command='(bash {path}/ec_itemsold_sale_import.sh {lastday})'
        .format(path=path,lastday=get_date()[0]),
    ssh_hook=sshHook,
    dag=dag)

spark_daysale = SSHExecuteOperator(
    task_id="itemsold_daysale_parse",
    bash_command='(bash {path}/ec_itemsold_daysale_parse.sh {last_2_days} {lastday})'
        .format(path=path,last_2_days=get_date()[1],lastday=get_date()[0]),
    ssh_hook=sshHook,
    dag=dag)

hive_daysale = SSHExecuteOperator(
    task_id="itemsold_daysale_import",
    bash_command='(bash {path}/ec_itemsold_daysale_import.sh {last_2_days})'
        .format(path=path,last_2_days=get_date()[1]),
    ssh_hook=sshHook,
    dag=dag)

email = EmailOperator(task_id='itemsold_sale_daysale_email',
                      to=['lienlian@wolongdata.com','wangruitong@wolongdata.com'],
                      subject='itemsold sale&daysale workflow',
                      html_content='[ itemsold sale&daysale data updated!!! ]',
                      dag=dag)
chain(spark_sale,hive_sale,spark_daysale,hive_daysale,email)
