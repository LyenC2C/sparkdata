#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/shixin/'
now_day=$1
#last_day=$2

hfs -rmr /user/wrt/temp/ppd_info_tmp
spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 $dev_path/ppd/t_base_ppd_info.py

hive<<EOF

use wlcredit;

LOAD DATA INPATH '/user/wrt/temp/ppd_info_tmp' OVERWRITE INTO TABLE t_wrt_shixin_qiye PARTITION (ds=$now_day);

EOF
