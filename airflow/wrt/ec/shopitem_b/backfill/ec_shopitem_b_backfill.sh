#! /bin/bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
lastday=$1
last_2_days=$2

hadoop fs -test -e /user/wrt/shopitem_tmp
if [ $? -eq 0 ] ;then
hadoop fs  -rmr /user/wrt/shopitem_tmp
else
echo 'Directory is not exist,you can run you spark job as you want!!!'
fi


spark-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 \
$pre_path/wrt/data_base_process/t_base_shopitem_b.py $lastday $last_2_days

hive<<EOF
set hive.merge.mapredfiles = true;
set hive.merge.mapfiles = true;
set hive.merge.size.per.task = 240000000;
set hive.merge.smallfiles.avgsize= 180000000;
LOAD DATA  INPATH '/user/wrt/shopitem_tmp' OVERWRITE INTO TABLE wl_base.t_base_ec_shopitem_b PARTITION (ds=$lastday);
EOF
