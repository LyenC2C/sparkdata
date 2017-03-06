#!/bin/bash

#xianyu_iteminfo_stat
source ~/.bashrc

logs_location="/home/lel/airflow/logs"
lastday_res=`ls $logs_location/shopitem_c/shopitem_c_import | sort -r | awk 'NR==1{print $1}'`
last_2_day_res=`ls $logs_location/shopitem_c/shopitem_c_import | sort -r | awk 'NR==2{print $1}'`

lastday_str=`tail -10 $logs_location/shopitem_c/shopitem_c_import/$lastday_res | grep Partition`
last_2_day_str=`tail -10 $logs_location/shopitem_c/shopitem_c_import/$last_2_day_res | grep Partition`

echo t_base_ec_shopitem_c    $lastday_str    $last_2_days_str
