#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
zuotian=$(date -d '1 days ago' +%Y%m%d)
qiantian=$(date -d '2 days ago' +%Y%m%d)

hadoop fs -test -e /user/wrt/daysale_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/daysale_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi

spark-submit  --total-executor-cores  120   --executor-memory  7g  --driver-memory 7g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian