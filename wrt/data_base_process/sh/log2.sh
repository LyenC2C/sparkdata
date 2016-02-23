#!/usr/bin/env bash
pre_path='/home/wrt/sparkdata'

qiantian='20151130'
zuotian='20151201'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151201'
zuotian='20151202'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151202'
zuotian='20151203'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151203'
zuotian='20151204'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151204'
zuotian='20151205'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151205'
zuotian='20151206'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151206'
zuotian='20151207'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151207'
zuotian='20151208'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151208'
zuotian='20151209'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151209'
zuotian='20151210'
#
hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151210'
zuotian='20151212'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151212'
zuotian='20151212'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151212'
zuotian='20151213'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151213'
zuotian='20151214'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151214'
zuotian='20151215'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 20g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1