#!/bin/bash

source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)

hadoop fs -test -e /user/lel/temp/xianyu_itemcomment
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/lel/temp/xianyu_itemcomment
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 60 $work/spark/xianyu/xianyu_itemcomment.py $lastday