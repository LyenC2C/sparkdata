#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/shixin/'
now_day=$(date -d '0 days ago' +%Y%m%d)

hadoop fs -test -e /user/wrt/temp/shixin_qiyeinfo
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/temp/shixin_qiyeinfo
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 $dev_path/shixin_qiye.py $now_day
