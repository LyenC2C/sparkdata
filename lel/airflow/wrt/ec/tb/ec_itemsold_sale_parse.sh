#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
zuotian=$(date -d '1 days ago' +%Y%m%d)
qiantian=$(date -d '2 days ago' +%Y%m%d)
iteminfo_day='20170114'

hadoop fs -test -e /user/wrt/sale_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/sale_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 7G  --driver-memory 7G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $qiantian $zuotian $iteminfo_day
