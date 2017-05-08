#!/bin/bash

source ~/.bashrc

latest=`hadoop fs -ls /hive/warehouse/wl_base.db/t_base_record_data_backflow | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
path=/home/lel/sparkdata/lel/spark/data_backflow/api_records

spark-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 $path/data_standard_s2.py $latest
