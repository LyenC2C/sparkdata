#!/bin/bash

#xianyu_iteminfo_stat
source ~/.bashrc

table=wl_base.t_base_ec_xianyu_iteminfo

#lastday
lastday=`hadoop fs -ls /hive/warehouse/wl_base.db/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
lastday_size=`hadoop fs -du -s  /hive/warehouse/wl_base.db/$table/ds=$lastday | awk '{print $1/1024/1024" ""MB"}'`
lastday_files=`hadoop fs -ls /hive/warehouse/wl_base.db/$table/ds=$lastday | wc -l`
lastday_rows=`impala-shell -k -i cs104 -q "SELECT count(*) FROM $table where ds=$lastday"`

#last_2_days
last_2_days=`hadoop fs -ls /hive/warehouse/wl_base.db/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==2{print $0}'`
last_2_days_size=`hadoop fs -du -s  /hive/warehouse/wl_base.db/$table/ds=$last_2_days | awk '{print $1/1024/1024" ""MB"}'`
last_2_days_rows=last_2_days_rows=`impala-shell -k -i cs104 -q "SELECT count(*) FROM $table where ds=$$last_2_days"`
last_2_days_files=`hadoop fs -ls /hive/warehouse/wl_base.db/$table/ds=$last_2_days | wc -l`

echo \{\"table\":\"$table\",\"update_day\":\{\"update_day\":\"$lastday\",\"total_files\":\"$lastday_files\",\"total_rows\":\"$lastday_rows\",\"total_size\":\"$last_2_days_size\"\},\"last_update_day\":\{\"last_update_day\":\"$last_2_days\",\"total_files\":\"$last_2_days_files\",\"total_rows\":\"$last_2_days_rows\",\"total_size\":\"$last_2_days_files\"\}\}

#logs_location="/home/lel/airflow/logs"
#lastday_res=`ls $logs_location/xianyu_iteminfo/iteminfo_import | sort -r | awk 'NR==1{print $1}'`
#last_2_day_res=`ls $logs_location/xianyu_iteminfo/iteminfo_import | sort -r | awk 'NR==2{print $1}'`
#lastday_str=`tail -10 $logs_location/xianyu_iteminfo/iteminfo_import/$lastday_res | grep Partition`
#last_2_day_str=`tail -10 $logs_location/xianyu_iteminfo/iteminfo_import/$last_2_day_res | grep Partition`
#echo t_base_ec_xianyu_iteminfo    $lastday_str    $last_2_days_str


