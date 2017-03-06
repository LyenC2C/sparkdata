#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/ppd/'
now_day=$1
#last_day=$2

hadoop fs -rmr /user/wrt/temp/ppd_info_tmp
spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 \
$dev_path/t_base_ppd_listinfo.py $now_day

hive<<EOF

use wlcredit;

LOAD DATA INPATH '/user/wrt/temp/ppd_info_tmp' OVERWRITE INTO TABLE t_base_credit_ppd_listinfo PARTITION (ds=$now_day);

EOF
