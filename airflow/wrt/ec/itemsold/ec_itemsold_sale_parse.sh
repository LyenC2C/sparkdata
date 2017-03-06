#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

#zuotian=$(date -d '1 days ago' +%Y%m%d)
#qiantian=$(date -d '2 days ago' +%Y%m%d)
#iteminfo_day='20170114'
lastday=$1
last_2_days=$2
iteminfo_day=$3

hadoop fs -test -e /user/wrt/sale_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/sale_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --driver-memory 6G --num-executors 15 --executor-memory 15G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $lastday $last_2_days $iteminfo_day
