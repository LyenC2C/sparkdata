from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'lyen',
    'depends_on_past': False,
    'start_date': datetime(2016, 12, 19,10,30),
    'email': ['lienlian@wolongdata.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'end_date': datetime(2016, 12, 19,10,35),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
}

dag = DAG('hive_spark', default_args=default_args, schedule_interval=timedelta(minutes=10))

task_bash = BashOperator(task_id='task_bash',
                         bash_command='date',
                         dag=dag)
hive_sql = "create table wc_2 as \
select w.word as word,count(1) as count from \
(select explode(split(id,' ')) as word from wc_text) w \
group by word order by count desc;"
task_hive = HiveOperator(task_id='hive_wc',
                         hql=hive_sql,
                         dag=dag)

task_hive.set_upstream(task_bash)

task_pyspark_cmd = 'pyspark --executor-memory 1G  --driver-memory 1G  --total-executor-cores 1 /home/lyen/wordcount.py'
task_pyspark_wc = BashOperator(task_id='pyspark_wc',
                               bash_command=task_pyspark_cmd,
                               dag=dag)
task_hive.set_downstream(task_pyspark_wc)
