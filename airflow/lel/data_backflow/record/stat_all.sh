#!/bin/bash
source ~/.bashrc

HOSTNAME="10.3.4.104"
PORT="3306"
USERNAME="airflow"
PASSWORD="airflow"
DBNAME="airflow"
TABLENAME="stats"

table=t_lel_record_data_backflow_multifields_standard_res
database=wl_service
db_path=$database.db


refresh=`impala-shell -k -s hive -i cs107 -q "refresh $database.$table"`
lastday_size=`hadoop fs -du -s  /hive/warehouse/$db_path/$table | awk '{print $1/1024/1024}'`
lastday_files=`hadoop fs -ls /hive/warehouse/$db_path/$table | wc -l`
lastday_avg_size=`awk 'BEGIN{print ('$lastday_size'/'$lastday_files')}'`
lastday_rows=`impala-shell -k  -s hive  -i cs107 -q "SELECT count(*) FROM $database.$table " | grep -oP '(\d+)'`
today=$(date -d '0 days ago' +%Y%m%d)

use_db_sql="use ${DBNAME}"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} -e "${use_db_sql}"
insert_sql="insert into ${TABLENAME} values(null,'$database','$table','$lastday_size','$lastday_files','$lastday_avg_size','$lastday_rows','$today')"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"








