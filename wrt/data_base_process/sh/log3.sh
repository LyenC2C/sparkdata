#!/usr/bin/env bash
pre_path='/home/wrt/sparkdata'
#zuotian=$(date -d '1 days ago' +%Y%m%d)

qiantian='20160125'
zuotian='20160126'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160216 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160126'
zuotian='20160127'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160216 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160127'
zuotian='20160128'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160216 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160128'
zuotian='20160129'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160216 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1

qiantian='20160129'
zuotian='20160130'

hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian 20160216 >> ./log_date/log_$zuotian 2>&1
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1



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


#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151026
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151027
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151028
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151029
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151030
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151102
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151104
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151105
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151106
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151107
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151108
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151109
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151110
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151111
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151112
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151113
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151114
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151115
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151116
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151117
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151118
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151119
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151120
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151121
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151122
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151123
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151124
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151125
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151126
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151127
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151128
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151129
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151130
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151201
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151202
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151203
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151204
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151205
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151206
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151207
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151208
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151209
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151210
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151211
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151212
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151213
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151214
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151215
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151216
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151217
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151218
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151219
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/wrt/data_base_process/hebing.py 20151220

#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  60   --executor-memory  6g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151220/*  20151219  20151220
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151220

#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151213/*  20151212 20151213

#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151214/*  20151213 20151214
#
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151215/*  20151214 20151215
#
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151216/*  20151215 20151216
#
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151217/*  20151216 20151217

