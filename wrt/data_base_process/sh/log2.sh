#!/usr/bin/env bash
pre_path='/home/wrt/sparkdata'

qiantian='20160309'
zuotian='20160310'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160310'
zuotian='20160311'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160311'
zuotian='20160312'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160312'
zuotian='20160313'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160313'
zuotian='20160314'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160314'
zuotian='20160315'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20160315'
zuotian='20160316'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

#qiantian='20160131'
#zuotian='20160201'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160201'
#zuotian='20160202'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160202'
#zuotian='20160203'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160203'
#zuotian='20160204'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160204'
#zuotian='20160205'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160205'
#zuotian='20160206'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160206'
#zuotian='20160207'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160207'
#zuotian='20160208'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160208'
#zuotian='20160209'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160209'
#zuotian='20160210'
##
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160210'
#zuotian='20160211'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160211'
#zuotian='20160212'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160212'
#zuotian='20160213'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160213'
#zuotian='20160214'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160214'
#zuotian='20160215'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160215'
#zuotian='20160216'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160216'
#zuotian='20160217'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160217'
#zuotian='20160218'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160218'
#zuotian='20160219'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160219'
#zuotian='20160220'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160220'
#zuotian='20160221'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160221'
#zuotian='20160222'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160222'
#zuotian='20160223'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160223'
#zuotian='20160224'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160224'
#zuotian='20160225'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160225'
#zuotian='20160226'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160226'
#zuotian='20160227'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160227'
#zuotian='20160228'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160228'
#zuotian='20160229'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160229'
#zuotian='20160301'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160301'
#zuotian='20160302'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160302'
#zuotian='20160303'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160303'
#zuotian='20160304'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160304'
#zuotian='20160305'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160305'
#zuotian='20160306'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160306'
#zuotian='20160307'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160307'
#zuotian='20160308'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160308'
#zuotian='20160309'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

#qiantian='20160229'
#zuotian='20160230'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20160230'
#zuotian='20160231'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1