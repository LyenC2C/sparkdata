#!/bin/bash

source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
path=/home/lel/sparkdata/lel/spark/data_backflow/api_records

hadoop fs -test -e /user/lel/temp/record_data_backflow
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/record_data_backflow
else
    echo 'Directory is not exist,you can run your spark as you want!!!'
fi

spark2-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 $path/data_parse_oldway_s1.py $today

hadoop fs -chmod -R 777 /user/lel/temp/record_data_backflow
