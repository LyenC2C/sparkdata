#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
zuotian=$(date -d '1 days ago' +%Y%m%d)
qiantian=$(date -d '2 days ago' +%Y%m%d)

hadoop fs -test -e /user/wrt/shopitem_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/shopitem_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 60 \
$pre_path/wrt/data_base_process/t_base_shopitem_b.py $zuotian $qiantian>> \
$pre_path/wrt/data_base_process/sh/log_shopitem/log_$zuotian 2>&1

sh $pre_path/wrt/data_base_process/hive/t_base_shopitem_b.sql $zuotian


