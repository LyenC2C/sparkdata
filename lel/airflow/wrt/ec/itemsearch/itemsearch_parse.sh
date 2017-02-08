#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

lastday=$1

hadoop fs -test -e /user/wrt/temp/itemsearch
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/temp/itemsearch
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 10G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/itemsearch/t_base_item_search.py $lastday

spark-submit /home/wrt/sparkdata/wrt/data_base_process/itemsearch/t_base_item_search.py 20170208