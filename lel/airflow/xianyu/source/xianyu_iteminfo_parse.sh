#!/bin/bash

source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
thedaybeforelastday=$(date -d '2 days ago' +%Y%m%d)

hadoop_env=/home/lel/hadoop/bin
spark_env=/home/lel/spark/bin
work_place=/home/lel/wolong/sparkdata/lel

$hadoop_env/hadoop fs -test -e /user/lel/temp/xianyu_comment_2016
if [ $? -eq 0 ] ;then
$hadoop_env/hadoop fs  -rmr /user/lel/temp/xianyu_comment_2016
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

$spark_env/spark-submit  --executor-memory 6G  --driver-memory 6G  --total-executor-cores 80 $work_place/spark/xianyu/t_xianyu.py $lastday