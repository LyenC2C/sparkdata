#!/bin/bash
source ~/.bashrc

#create table stats(id int not null auto_increment primary key,d_name varchar(50) default 'wl_base',t_name varchar(50) not null,total_size varchar(200),total_files varchar(200),avg_size varchar(200),rows varchar(200),date varchar(20));
HOSTNAME="10.3.4.104"
PORT="3306"
USERNAME="airflow"
PASSWORD="airflow"
DBNAME="airflow"
TABLENAME="stats"

table=t_base_ec_item_daysale_dev_new
database=wl_base
db_path=$database.db

refresh=`impala-shell -k -i cs107 -q "refresh $database.$table"`
#lastday
lastday=`hadoop fs -ls /hive/warehouse/$db_path/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
lastday_size=`hadoop fs -du -s  /hive/warehouse/$db_path/$table/ds=$lastday | awk '{print $1/1024/1024}'`
lastday_files=`hadoop fs -ls /hive/warehouse/$db_path/$table/ds=$lastday | wc -l`
lastday_avg_size=`awk 'BEGIN{print ('$lastday_size'/'$lastday_files')}'`
lastday_rows=`impala-shell -k -i cs107 -q "SELECT count(*) FROM $database.$table where ds='$lastday'" | grep -oP '(\d+)'`


use_db_sql="use ${DBNAME}"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} -e "${use_db_sql}"

insert_sql="insert into ${TABLENAME} values(null,'$database','$table','$lastday_size','$lastday_files','$lastday_avg_size','$lastday_rows','$lastday')"
mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e "${insert_sql}"








