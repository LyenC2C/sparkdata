#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
#zuotian=$(date -d '1 days ago' +%Y%m%d)
#qiantian=$(date -d '2 days ago' +%Y%m%d)
#iteminfo_day='20161013'

zuotian=$1
qiantian=$2
iteminfo_day=$3

#hfs -rmr /user/wrt/sale_tmp >> $pre_path/wrt/data_base_process/sh/log_date/log_$zuotian 2>&1
#
#spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_base_item_sale.py $qiantian $zuotian $iteminfo_day >> \
#$pre_path/wrt/data_base_process/sh/log_date/log_$zuotian 2>&1
#
#sh $pre_path/wrt/data_base_process/hive/t_base_item_sale.sql $zuotian >> \
#$pre_path/wrt/data_base_process/sh/log_date/log_$zuotian 2>&1


hadoop fs -rm -r /user/wrt/daysale_tmp >> $pre_path/wrt/data_base_process/sh/log_daysale/log_$qiantian 2>&1

spark-submit  --total-executor-cores  120   --executor-memory  9g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> \
$pre_path/wrt/data_base_process/sh/log_daysale/log_$qiantian 2>&1

sh $pre_path/wrt/data_base_process/hive/cal_daysale.sql $qiantian >> \
$pre_path/wrt/data_base_process/sh/log_daysale/log_$qiantian 2>&1
