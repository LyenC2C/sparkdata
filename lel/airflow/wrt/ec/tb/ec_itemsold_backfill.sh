#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
zuotian=$(date -d '1 days ago' +%Y%m%d)
qiantian=$(date -d '2 days ago' +%Y%m%d)
iteminfo_day='20170121'

hfs -rmr /user/wrt/sale_tmp

spark-submit  --executor-memory 7G  --driver-memory 7G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $2 $1 $3

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/sale_tmp' OVERWRITE INTO TABLE t_base_ec_item_sold_dev PARTITION (ds='$1');
EOF

hadoop fs -rm -r /user/wrt/daysale_tmp

spark-submit  --total-executor-cores  120   --executor-memory  7g  --driver-memory 7g \
$pre_path/wrt/data_base_process/cal_daysale.py $2 $1

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE t_base_ec_item_daysale_dev_new PARTITION (ds='$2');
EOF