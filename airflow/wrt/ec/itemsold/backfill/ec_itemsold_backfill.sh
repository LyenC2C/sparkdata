#!/bin/bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
lastday=$1
last_2_days=$2
iteminfoday=$3

hadoop fs -rmr /user/wrt/sale_tmp

spark-submit  --driver-memory 8G --num-executors 40 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $last_2_days $lastday $iteminfoday

echo $1
echo $2
hadoop fs -chmod -R 777 /user/wrt/sale_tmp

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
LOAD DATA  INPATH "/user/wrt/sale_tmp" OVERWRITE INTO TABLE wl_base.t_base_ec_item_sold_dev PARTITION (ds=$lastday);
EOF

hadoop fs -rm -r /user/wrt/daysale_tmp

spark-submit  --driver-memory 8G --num-executors 40 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/cal_daysale.py $2 $1

hadoop fs -chmod -R 777 /user/wrt/daysale_tmp

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
LOAD DATA  INPATH "/user/wrt/daysale_tmp" OVERWRITE INTO TABLE wl_base.t_base_ec_item_daysale_dev_new PARTITION (ds=$last_2_days);
EOF


