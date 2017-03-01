#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
lastday=$1
last_2_days=$2

#hadoop fs  -rmr /user/wrt/shopitem_c_tmp
#
#spark-submit --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 \
#$pre_path/wrt/data_base_process/t_base_shopitem_c.py $lastday
#
#table=wl_base.t_base_ec_shopitem_c

hive<<EOF
LOAD DATA  INPATH '/user/wrt/shopitem_c_tmp' OVERWRITE INTO TABLE $table PARTITION (ds='0temp');
EOF

