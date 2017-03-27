#!/bin/bash
source ~/.bashrc

pre_path='/home/wrt/sparkdata'


hadoop fs -test -e /user/wrt/temp/iteminfo_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/temp/iteminfo_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark2-submit  --driver-memory 6G --num-executors 20 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_item_info.py -spark

hadoop fs -chmod -R 777 /user/wrt/temp/iteminfo_tmp

