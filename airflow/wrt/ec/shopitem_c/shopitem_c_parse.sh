#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
lastday=$1

hadoop fs -test -e /user/wrt/shopitem_c_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/shopitem_c_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark2-submit  --driver-memory 6G --num-executors 15 --executor-memory 15G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_shopitem_c.py $lastday

hadoop fs -chmod -R 777 /user/wrt/shopitem_c_tmp

