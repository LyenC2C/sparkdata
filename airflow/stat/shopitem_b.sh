#!/bin/bash

#xianyu_itemcomment_stat
source ~/.bashrc


#lastday
lastday=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shopitem_b | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
lastday_size=`hadoop fs -du -s  /hive/warehouse/wl_base.db/t_base_ec_shopitem_b/ds=$lastday | awk '{print $1/1024/1024" ""MB"}'`
lastday_files=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shopitem_b/ds=$lastday | wc -l`
lastday_rows=`hive -S -e "select count(1) from wl_base.t_base_ec_shopitem_b where ds=$lastday"`

#last_2_days
last_2_days=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shopitem_b | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==2{print $0}'`
last_2_days_size=`hadoop fs -du -s  /hive/warehouse/wl_base.db/t_base_ec_shopitem_b/ds=$last_2_days | awk '{print $1/1024/1024" ""MB"}'`
last_2_days_rows=`hive -S -e "select count(1) from wl_base.t_base_ec_shopitem_b where ds=$last_2_days"`
last_2_days_files=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_ec_shopitem_b/ds=$last_2_days | wc -l`

echo t_base_ec_shopitem_b    $lastday    $lastday_files    $last_2_days_rows    $last_2_days_size
echo t_base_ec_shopitem_b    $last_2_days    $last_2_days_files    $last_2_days_rows    $last_2_days_files

