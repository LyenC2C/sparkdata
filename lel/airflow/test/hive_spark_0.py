from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime,timedelta


default_args = {
    'owner': 'lyen',
    'depends_on_past': False,
    'start_date': datetime(2016,12,21),
    'email': ['lienlian@wolongdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retries': 1,
    #'retry_delay': timedelta(minutes=2),
    #'end_date': datetime(2016, 12, 21),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG('hive_spark',default_args=default_args,schedule_interval=timedelta(minutes=5))

task_bash = BashOperator(task_id='print_date',
                      bash_command='date',
                      dag=dag)
task_hive_sql = 'create table lel.wc_2 as select w.word as word,count(1) as count from (select explode(split(id,' ')) as word from lel.wc_text) w group by word order by count desc'
task_hive = HiveOperator(task_id='hive_task',
                         hql=task_hive_sql,
                         hive_cli_conn_id='',
                         dag=dag)
task_hive.set_downstream(task_bash)
task_spark_cmd = 'pyspark --executor-memory 1G  --driver-memory 1G  --total-executor-cores 1 /home/lyen/wordcount.py'
task_spark = BashOperator(task_id='pyspark_task',
                      bash_command=task_spark_cmd,
                      dag=dag)
task_spark.set_downstream(task_hive)
