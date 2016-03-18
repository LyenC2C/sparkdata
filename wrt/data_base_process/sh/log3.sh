#!/usr/bin/env bash
pre_path='/home/wrt/sparkdata'
#zuotian=$(date -d '1 days ago' +%Y%m%d)


qiantian='20160309'
zuotian='20160310'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160310'
zuotian='20160311'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160311'
zuotian='20160312'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160312'
zuotian='20160313'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160313'
zuotian='20160314'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160314'
zuotian='20160315'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160315'
zuotian='20160316'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#qiantian='20160131'
#zuotian='20160201'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160229'
#zuotian='20160301'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160301'
#zuotian='20160302'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
##
#qiantian='20160302'
#zuotian='20160303'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
##
#qiantian='20160303'
#zuotian='20160304'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
##
#qiantian='20160304'
#zuotian='20160305'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160305'
#zuotian='20160306'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160306'
#zuotian='20160307'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160307'
#zuotian='20160308'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160308'
#zuotian='20160309'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160209'
#zuotian='20160210'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160210'
#zuotian='20160211'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160211'
#zuotian='20160212'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160212'
#zuotian='20160213'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160213'
#zuotian='20160214'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160214'
#zuotian='20160215'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160215'
#zuotian='20160216'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160216'
#zuotian='20160217'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160217'
#zuotian='20160218'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160218'
#zuotian='20160219'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160219'
#zuotian='20160220'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160220'
#zuotian='20160221'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160221'
#zuotian='20160222'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#qiantian='20160222'
#zuotian='20160223'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160223'
#zuotian='20160224'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160224'
#zuotian='20160225'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160225'
#zuotian='20160226'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160226'
zuotian='20160227'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160227'
zuotian='20160228'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1


qiantian='20160228'
zuotian='20160229'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1


#qiantian='20160125'
#zuotian='20160126'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160126'
#zuotian='20160127'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160127'
#zuotian='20160128'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#qiantian='20160128'
#zuotian='20160129'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160129'
#zuotian='20160130'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#qiantian='20160130'
#zuotian='20160131'
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160225 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1





#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151216 20151217 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151217 20151218 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151218 20151219 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151219 20151220 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151220 20151221 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151221 20151222 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151222 20151223 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151223 20151224 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151224 20151225 20151224 20151224


#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151026
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151027
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151028
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151029
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151030
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151102
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151104
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151105
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151106
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151107
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151108
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151109
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151110
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151111
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151112
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151113
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151114
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151115
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151116
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151117
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151118
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151119
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015180
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151121
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151122
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151123
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151124
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151125
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151126
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151127
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151128
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151129
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151130
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015801
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015802
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015803
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015804
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015805
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015806
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015807
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015808
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 2015809
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151210
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151211
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151212
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151213
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151214
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151215
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151216
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151217
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151218
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151219
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/wrt/data_base_process/hebing.py 20151220

#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  60   --executor-memory  6g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151220/*  20151219  20151220
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151220

#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151213/*  20151212 20151213

#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151214/*  20151213 20151214
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151215/*  20151214 20151215
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151216/*  20151215 20151216
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151217/*  20151216 20151217

