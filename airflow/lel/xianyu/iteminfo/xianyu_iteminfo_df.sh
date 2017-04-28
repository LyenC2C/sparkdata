#!/bin/bash

source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
last_2day=$(date -d '2 days ago' +%Y%m%d)

spark2-submit  --driver-memory 4G --num-executors 20 --executor-memory 20G --executor-cores 5\
/home/lel/sparkdata/lel/spark/xianyu/xianyu_iteminfo_df.py $lastday $last_2day
