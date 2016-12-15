#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
#zuotian=$(date -d '1 days ago' +%Y%m%d)
#qiantian=$(date -d '2 days ago' +%Y%m%d)
#zuotian='20161204'
#qiantian='20161203'
#zuotian=$1
#qiantian=$2
now_day=$1
last_day=$2

hfs -rmr /user/wrt/shopitem_tmp >> $pre_path/wrt/data_base_process/sh/log_shopitem/log_c_$now_day 2>&1

spark-submit  --executor-memory 6G  --driver-memory 5G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_base_shopitem_c.py $now_day $last_day >> \
$pre_path/wrt/data_base_process/sh/log_shopitem/log_c_$now_day 2>&1

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_c PARTITION (ds='$now_day');
EOF

hfs -mv /commit/shopitem_c/20* /commit/shopitem_c/past_day/