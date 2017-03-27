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

spark2-submit  --driver-memory 8G --num-executors 20 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/itemsearch/t_base_item_search.py $lastday

hadoop fs -chmod -R 777 /user/wrt/temp/itemsearch