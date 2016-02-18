#!/bin/sh
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
today='20151126'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151127'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151128'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151129'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151130'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151201'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151202'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151203'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151204'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151205'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151206'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151207'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151208'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151209'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151210'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151211'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151212'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151213'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151214'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151215'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151216'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151217'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151218'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151219'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151220'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151221'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151222'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151223'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151224'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151225'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151226'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151227'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151228'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151229'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151230'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151231'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  40  --executor-memory 4g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1