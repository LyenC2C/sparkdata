#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
lastday=$(date -d '7 days ago' +%Y%m%d)

pre_path='/home/wrt/sparkdata'


hadoop fs -test -e /user/wrt/temp/iteminfo_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/temp/iteminfo_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_info.py -spark

