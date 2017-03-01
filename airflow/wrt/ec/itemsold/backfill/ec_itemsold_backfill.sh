#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

hadoop fs -rmr /user/wrt/sale_tmp

spark-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $2 $1 $3

hive<<EOF
use wl_base;
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds='$1');
EOF

hadoop fs -rm -r /user/wrt/daysale_tmp

spark-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/cal_daysale.py $2 $1

hive<<EOF
use wl_base;
LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds='$2');
EOF

