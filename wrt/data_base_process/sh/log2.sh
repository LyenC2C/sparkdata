#!/usr/bin/env bash
pre_path='/home/wrt/sparkdata'

qiantian='20151230'
zuotian='20151231'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151231'
zuotian='20160101'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1


qiantian='20160101'
zuotian='20160102'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160102'
zuotian='20160103'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160103'
zuotian='20160104'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160104'
zuotian='20160105'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160105'
zuotian='20160106'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160106'
zuotian='20160107'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160107'
zuotian='20160108'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160108'
zuotian='20160109'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160109'
zuotian='20160110'
#
hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160110'
zuotian='20160111'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160111'
zuotian='20160112'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160112'
zuotian='20160113'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160113'
zuotian='20160114'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160114'
zuotian='20160115'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160115'
zuotian='20160116'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160116'
zuotian='20160117'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160117'
zuotian='20160118'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='201601118'
zuotian='20160119'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160119'
zuotian='20160120'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160120'
zuotian='20160121'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160121'
zuotian='20160122'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160122'
zuotian='20160123'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160123'
zuotian='20160124'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160124'
zuotian='20160125'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160125'
zuotian='20160126'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160126'
zuotian='20160127'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160127'
zuotian='20160128'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160128'
zuotian='20160129'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160129'
zuotian='20160130'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160130'
zuotian='20160131'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  120   --executor-memory  01g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1