#!/usr/bin/env bash
source ~/.bashrc
dev_path='/home/wrt/sparkdata/wrt/credit/ppd/'
now_day=$(date -d '0 days ago' +%Y%m%d)

hadoop fs -test -e /user/wrt/temp/ppd_info_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/temp/ppd_info_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --executor-memory 6G  --driver-memory 8G  --total-executor-cores 80 \
$dev_path/t_base_ppd_listinfo.py $now_day

