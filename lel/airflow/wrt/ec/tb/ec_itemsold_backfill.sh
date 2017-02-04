#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

hadoop fs -rmr /user/wrt/sale_tmp

spark-submit  --executor-memory 10G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $2 $1 $3

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds='$1');
EOF

hadoop fs -rm -r /user/wrt/daysale_tmp

spark-submit  --total-executor-cores  120   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/cal_daysale.py $2 $1

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds='$2');
EOF