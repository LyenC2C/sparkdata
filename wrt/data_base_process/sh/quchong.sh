pre_path='/home/wrt/sparkdata'

#qiantian='20151028'
#zuotian='20151029'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

#qiantian='20151029'
#zuotian='20151030'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151030'
#zuotian='20151102'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151102'
#zuotian='20151104'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151104'
#zuotian='20151105'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

#qiantian='20151105'
#zuotian='20151106'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151106'
#zuotian='20151107'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151107'
#zuotian='20151108'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151108'
#zuotian='20151109'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1
#
#qiantian='20151109'
#zuotian='20151110'
#
#hadoop fs -rm -r /user/wrt/daysale_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
#sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151110'
zuotian='20151111'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151111'
zuotian='20151112'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151112'
zuotian='20151113'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151113'
zuotian='20151114'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151114'
zuotian='20151115'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151115'
zuotian='20151116'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151116'
zuotian='20151117'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151117'
zuotian='20151118'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151118'
zuotian='20151119'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151119'
zuotian='2015180'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='2015180'
zuotian='20151121'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151121'
zuotian='20151122'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151122'
zuotian='20151123'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151123'
zuotian='20151124'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151124'
zuotian='20151125'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151125'
zuotian='20151126'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151126'
zuotian='20151127'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151127'
zuotian='20151128'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151128'
zuotian='20151129'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1

qiantian='20151129'
zuotian='20151130'

hadoop fs -rm -r /user/wrt/daysale_tmp
spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/wrt/data_base_process/cal_daysale.py $qiantian $zuotian >> ./log_daysale/log_$qiantian 2>&1
sh $pre_path/wrt/data_base_process/cal_daysale.sql $qiantian >> ./log_daysale/log_$qiantian 2>&1





#zuotian='20160122'
#qiantian='20160121'
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160111 >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160112 >> ./log_date/log_$zuotian 2>&1

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian 20160111 20160111 >> ./log_date/log_$zuotian 2>&1


#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20150131 20160101 20160106
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160101
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql 20150131 20160101 20160106 20160106
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20160101 20160102 20160106
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160102
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql 20160101 20160102 20160106 20160106
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20160102 20160103 20160106
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160103
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql 20160102 20160103 20160106 20160106
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20160103 20160104 20160106
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160104
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql 20160103 20160104 20160106 20160106
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20160104 20160105 20160106
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160105
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql 20160104 20160105 20160106 20160106
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20160105 20160106 20160106
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20160106
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql 20160105 20160106 20160106 20160106

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc  \
#/commit/iteminfo/20151226/*  20151225 20151226
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc  \
#/commit/iteminfo/20151227/*  20151226 20151227
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151226/*  20151225  20151226
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151226
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151227/*  20151226  20151227
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151227
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151221 20151222
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151222
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151222 20151223
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151223
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151223 20151224
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151224
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151224 20151225
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151225
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151225 20151226
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151226
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151226 20151227
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151227
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151221 20151222 20151227 20151227
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151222 20151223 20151227 20151227
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151223 20151224 20151227 20151227
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151224 20151225 20151227 20151227
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151225 20151226 20151227 20151227
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151226 20151227 20151227 20151227




#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc  \
#/commit/iteminfo/20151225/*  20151224 20151225
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151223/*  20151222  20151223
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151223

#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151224/*  20151223  20151224
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151224
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151225/*  20151224  20151225
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151225


#
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



#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8g  --driver-memory 20G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151222 20151221
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151222


#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151223/*  20151222 20151223
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151224/*  20151223 20151224

#sh $pre_path/zlj/project/base_data_process/hive/item/step.sh 20151219  20151220
#sh $pre_path/zlj/project/base_data_process/hive/item/step.sh 20151220  20151221
#sh $pre_path/zlj/project/base_data_process/hive/item/step.sh 20151221  20151222

#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151220/*  20151219  20151220
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151220
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151221/*  20151220  20151221
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151221
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151222/*  20151221  20151222
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151222

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151218 20151217
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151219 20151218
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151220 20151219

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151221 20151220
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151222 20151221
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151223 20151222

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151216 20151217 20151212 20151212


#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151218/*  20151217 20151218

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151217/*  20151216 20151217
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151219/*  20151217 20151219
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151220/*  20151219 20151220
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151221/*  20151220 20151221
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151222/*  20151221 20151222




#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151218 20151217
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151219 20151218
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151220 20151219


#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151212 20151211
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151213 20151212
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151214 20151213
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151215 20151214
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151216 20151215
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151217 20151216
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  2015809 20151210 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151210 20151211 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151211 20151212 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151212 20151213 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151213 20151214 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151214 20151215 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151215 20151216 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151216 20151217 20151212 20151212

#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151027
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151028
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151029
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151030
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151102
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151104
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151105
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151106
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151107
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151108
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151109
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151110
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151111
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151112
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151113
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151114
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151115
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151116
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151117
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151118
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151119
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015180
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151121
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151122
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151123
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151124
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151125
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151126
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151127
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151128
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151129
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151130
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015801
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015802
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015803
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015804
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015805
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015806
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015807
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015808
#spark-submit  --total-executor-cores  80  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 2015809

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/ 20151210

#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015806/*  2015805 2015806
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015807/*  2015806 2015807
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015808/*  2015807 2015808
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015809/*  2015808 2015809
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151210/*  2015809 20151210
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151211/*  20151210 20151211

#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151212/*  20151211 20151212
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151213/*  20151212 20151213
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151214/*  20151213 20151214
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151215/*  20151214 20151215

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 2015804 2015803
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015804/*  2015803 2015804
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/2015804/*  2015803 2015804

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151130 2015801 2015802 2015802
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  2015801 2015802 2015802 2015802

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015801/*  20151130 2015801
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015802/*  2015801 2015802
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/2015801/*  20151130 2015801
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/2015802/*  2015801 2015802

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151119 2015180 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  2015180 20151121 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151121 20151122 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151122 20151123 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151123 20151124 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151124 20151125 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151125 20151126 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151126 20151127 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151127 20151128 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151128 20151129 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151129 20151130 20151130 20151130

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151126/*  20151125 20151126
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151127/*  20151126 20151127
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151128/*  20151127 20151128
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151129/*  20151128 20151129
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151130/*  20151129 20151130

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151126/*  20151125 20151126
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151127/*  20151126 20151127

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151128/*  20151127 20151128
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151129/*  20151128 20151129
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151130/*  20151129 20151130

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151125 20151124
#
#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151126 20151125
#
#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151127 20151126
#
#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151128 20151127

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151130 20151129

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151118/*  20151117 20151118
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151119/*  20151118 20151119
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/2015180/*  20151119 2015180
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151121/*  2015180 20151121
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151122/*  20151121 20151122
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151123/*  20151122 20151123
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151124/*  20151123 20151124
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151125/*  20151124 20151125

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151121/*  2015180 20151121
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151122/*  20151121 20151122
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151123/*  20151122 20151123
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151124/*  20151123 20151124
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151125/*  20151124 20151125

#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151029 20151028
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151030 20151029
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151102 20151030
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151104 20151102
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151105 20151104
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151106 20151105
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151107 20151106
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151108 20151107
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151109 20151108
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151110 20151109
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151111 20151110
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151112 20151111
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151113 20151112
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151114 20151113
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151115 20151114
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151116 20151115
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151117 20151116

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151026 20151027 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151027 20151028 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151028 20151029 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151029 20151030 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151030 20151102 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151102 20151104 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151104 20151105 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151105 20151106 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151106 20151107 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151107 20151108 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151108 20151109 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151109 20151110 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151110 20151111 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151111 20151112 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151112 20151113 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151113 20151114 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151114 20151115 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151115 20151116 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151116 20151117 20151112 20151112

#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151111
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151112
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151113
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151114
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151115
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151116
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151117

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151026 20151027 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151027 20151028 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151028 20151029 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151029 20151030 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151030 20151102 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151102 20151104 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151104 20151105 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151105 20151106 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151106 20151107 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151107 20151108 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151108 20151109 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151109 20151110 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151110 20151111 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151111 20151112 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151112 20151113 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151113 20151115 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151115 20151116 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151116 20151117 20151112 20151112
