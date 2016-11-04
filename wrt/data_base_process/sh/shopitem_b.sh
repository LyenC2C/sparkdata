#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
zuotian=$(date -d '1 days ago' +%Y%m%d)
qiantian=$(date -d '2 days ago' +%Y%m%d)

#zuotian='20160919'
#qiantian='20160918'
#zuotian=$1
#qiantian=$2

hfs -rmr /user/wrt/shopitem_tmp >> $pre_path/wrt/data_base_process/sh/log_shopitem/log_$zuotian 2>&1

spark-submit  --executor-memory 6G  --driver-memory 5G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_base_shopitem_b.py $zuotian $qiantian>> \
$pre_path/wrt/data_base_process/sh/log_shopitem/log_$zuotian 2>&1

sh $pre_path/wrt/data_base_process/hive/t_base_shopitem_b.sql $zuotian >> \
$pre_path/wrt/data_base_process/sh/log_shopitem/log_$zuotian 2>&1
