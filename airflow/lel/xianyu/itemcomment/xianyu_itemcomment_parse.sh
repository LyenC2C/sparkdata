#!/bin/bash

source ~/.bashrc
date
date  +%Y%m%d

lastday=$1

hadoop fs -test -e /user/lel/temp/xianyu_itemcomment
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/lel/temp/xianyu_itemcomment
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --driver-memory 6G --num-executors 20 --executor-memory 20G --executor-cores 5 $work/spark/xianyu/xianyu_itemcomment.py $lastday