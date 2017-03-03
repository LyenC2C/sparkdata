#!/bin/bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

hadoop fs -rmr /user/wrt/sale_tmp

spark-submit  --driver-memory 6G --num-executors 15 --executor-memory 15G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $2 $1 $3

echo $1
echo $2

hive -e "LOAD DATA  INPATH '/user/wrt/sale_tmp' INTO TABLE wl_base.t_base_ec_item_sold_dev PARTITION (ds=$1);"

hadoop fs -rm -r /user/wrt/daysale_tmp

spark-submit  --driver-memory 6G --num-executors 15 --executor-memory 15G --executor-cores 5 \
$pre_path/wrt/data_base_process/cal_daysale.py $2 $1

hive -e "LOAD DATA  INPATH '/user/wrt/daysale_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_item_daysale_dev_new PARTITION (ds=$2);"


