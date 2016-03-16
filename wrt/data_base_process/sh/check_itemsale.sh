#!/bin/sh
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
today='20151230'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20151231'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160101'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160102'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160103'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160104'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160105'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160106'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160107'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160108'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160112'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160113'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160114'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160115'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160116'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160117'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160118'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160119'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160120'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160121'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160122'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160123'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1
today='20160124'
hadoop fs -rm -r /user/wrt/itemsale/ds=$today &> $pre_path/wrt/data_base_process/sh/check_log/log_$today
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/itemsale_check.py $today >> $pre_path/wrt/data_base_process/sh/check_log/log_$today 2>&1