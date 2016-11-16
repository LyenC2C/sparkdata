#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

spark-submit --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120\
$pre_path/wrt/data_base_process/repair/repair_shopitem_b_sold.py 20160908 20160907
#spark-submit --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120\
#./repair_shopitem_b_sold.py 20160909 20160908
#spark-submit --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120\
#./repair_shopitem_b_sold.py 20160909 20160908
