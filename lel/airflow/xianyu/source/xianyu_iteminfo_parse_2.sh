#!/bin/bash

source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
thedaybeforelastday=$(date -d '2 days ago' +%Y%m%d)


hadoop fs -test -e /user/lel/temp/xianyu_comment_2016
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/lel/temp/xianyu_comment_2016
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 80 $work/spark/xianyu/t_xianyu.py $lastday