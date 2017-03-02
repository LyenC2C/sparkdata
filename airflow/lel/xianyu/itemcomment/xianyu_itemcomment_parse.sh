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

spark-submit  --driver-memory 4G --num-executors 10 --executor-memory 10G --executor-cores 4 $work/spark/xianyu/xianyu_itemcomment.py $lastday