#!/bin/bash
source ~/.bashrc

pre_path='/home/lel/sparkdata'


hadoop fs -test -e /user/lel/temp/iteminfo_abnormal
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/lel/temp/iteminfo_abnormal
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark2-submit  --driver-memory 6G --num-executors 20 --executor-memory 20G --executor-cores 5 \
$pre_path/lel/spark/iteminfo_abnormal/iteminfo_abnormal.py -spark

hadoop fs -chmod -R 777 /user/lel/temp/iteminfo_abnormal

