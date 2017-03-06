#!/bin/bash

#xianyu_iteminfo_stat
source ~/.bashrc

logs_location="/home/lel/airflow/logs"

#t_base_ec_item_dev_new
#lastday
lastday=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_item_dev_new | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
lastday_size=`hadoop fs -du -s  /hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=$lastday | awk '{print $1/1024/1024" ""MB"}'`
lastday_files=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=$lastday | wc -l`
lastday_rows=`hive -S -e "select count(1) from wl_base.t_base_ec_item_dev_new where ds=$lastday"`

#last_2_days
last_2_days=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_item_dev_new | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==2{print $0}'`
last_2_days_size=`hadoop fs -du -s  /hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=$last_2_days | awk '{print $1/1024/1024" ""MB"}'`
last_2_days_rows=`hive -S -e "select count(1) from wl_base.t_base_ec_item_dev_new where ds=$last_2_days"`
last_2_days_files=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_item_dev_new/ds=$last_2_days | wc -l`

#t_base_ec_shop_dev_new
#lastday_shop
lastday_shop=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
lastday_size_shop=`hadoop fs -du -s  /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new/ds=$lastday | awk '{print $1/1024/1024" ""MB"}'`
lastday_files_shop=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new/ds=$lastday | wc -l`
lastday_rows_shop=`hive -S -e "select count(1) from wl_base.t_base_ec_shop_dev_new where ds=$lastday"`

#last_2_days_shop
last_2_days_shop=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==2{print $0}'`
last_2_days_size_shop=`hadoop fs -du -s  /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new/ds=$last_2_days | awk '{print $1/1024/1024" ""MB"}'`
last_2_days_rows_shop=`hive -S -e "select count(1) from wl_base.t_base_ec_shop_dev_new where ds=$last_2_days"`
last_2_days_files_shop=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shop_dev_new/ds=$last_2_days | wc -l`


echo t_base_ec_item_dev_new    $lastday    $lastday_files    $last_2_days_rows    $last_2_days_size
echo t_base_ec_item_dev_new    $last_2_days    $last_2_days_files    $last_2_days_rows    $last_2_days_files
echo t_base_ec_shop_dev_new    $lastday_daily    $lastday_files_daily    $last_2_days_rows_daily    $last_2_days_size_dailly
echo t_base_ec_shop_dev_new    $last_2_days_daily    $last_2_days_files_daily    $last_2_days_rows_daily    $last_2_days_files_daily


