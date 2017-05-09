#!/bin/bash

source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
path=/home/lel/sparkdata/lel/spark/data_backflow/api_records

hadoop fs -test -e /commit/data_backflow/datamart/stat_back/phone_idcard_stat_$today
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /user/lel/temp/record_data_backflow
else
    echo 'go ahead!'
fi
hadoop fs -test -e /commit/data_backflow/datamart/stat_back/stat_all_$today
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /commit/data_backflow/datamart/stat_back/stat_all_$today
else
    echo 'go ahead!'
fi

spark2-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5 $path/data_product.py $today

hadoop fs -chmod -R 777 /commit/data_backflow/datamart/stat_back/stat_all_$today
hadoop fs -chmod -R 777 /commit/data_backflow/datamart/stat_back/phone_idcard_stat_$today