#!/bin/bash

source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)

hadoop fs -test -e /user/lel/temp/xianyu_iteminfo
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/xianyu_iteminfo
else
    echo 'Directory is not exist,you can run your spark as you want!!!'
fi

spark-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 /home/lel/sparkdata/lel/spark/xianyu/xianyu_iteminfo.py $lastday
