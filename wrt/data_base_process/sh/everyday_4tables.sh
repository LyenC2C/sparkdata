#!/bin/sh
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
#zuotian=$(date -d '1 days ago' +%Y%m%d)
#qiantian=$(date -d '2 days ago' +%Y%m%d)
zuotian='20160105'
qiantian='20160104'

spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1

hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
>> ./log_date/log_$zuotian 2>&1
sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
