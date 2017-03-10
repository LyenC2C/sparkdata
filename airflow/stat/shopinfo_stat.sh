#!/bin/bash
source ~/.bashrc

table=t_base_ec_shop_dev_new
database=wl_base
db_path=$database.db

refresh=`impala-shell -k -i cs104 -q "refresh $database.$table"`
#lastday
lastday=`hadoop fs -ls /hive/warehouse$db_path/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
lastday_size=`hadoop fs -du -s  /hive/warehouse$db_path/$table/ds=$lastday | awk '{print $1/1024/1024" ""MB"}'`
lastday_files=`hadoop fs -ls /hive/warehouse$db_path/$table/ds=$lastday | wc -l`
lastday_rows=`impala-shell -k -i cs104 -q "SELECT count(*) FROM $database.$table where ds=$lastday"`

#last_2_days
last_2_days=`hadoop fs -ls /hive/warehouse$db_path/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==2{print $0}'`
last_2_days_size=`hadoop fs -du -s  /hive/warehouse$db_path/$table/ds=$last_2_days | awk '{print $1/1024/1024" ""MB"}'`
last_2_days_files=`hadoop fs -ls /hive/warehouse$db_path/$table/ds=$last_2_days | wc -l`
last_2_days_rows=`impala-shell -k -i  cs104 -q "SELECT count(*) FROM $database.$table where ds=$last_2_days"`


echo \{\"table\":\"$table\",\"update_day\":\{\"update_day\":\"$lastday\",\"total_files\":\"$lastday_files\",\"total_rows\":\"$lastday_rows\",\"total_size\":\"$lastday_size\"\},\"last_update_day\":\{\"last_update_day\":\"$last_2_days\",\"total_files\":\"$last_2_days_files\",\"total_rows\":\"$last_2_days_rows\",\"total_size\":\"$last_2_days_size\"\}\}










