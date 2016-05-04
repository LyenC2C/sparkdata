#!/bin/sh
source ~/.bashrc
pre_path='/home/wrt/sparkdata'
#zuotian=$(date -d '1 days ago' +%Y%m%d)
#qiantian=$(date -d '2 days ago' +%Y%m%d)

zuotian='20160429'
qiantian='20160428'

hfs -rmr /user/wrt/sale_tmp

spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $qiantian $zuotian 20160424


sh $pre_path/wrt/data_base_process/t_base_item_sale.sql $zuotian

zuotian='20160501'
qiantian='20160430'

hfs -rmr /user/wrt/sale_tmp
spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $qiantian $zuotian 20160424
sh $pre_path/wrt/data_base_process/t_base_item_sale.sql $zuotian

zuotian='20160502'
qiantian='20160501'

hfs -rmr /user/wrt/sale_tmp
spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $qiantian $zuotian 20160424
sh $pre_path/wrt/data_base_process/t_base_item_sale.sql $zuotian

zuotian='20160503'
qiantian='20160502'

hfs -rmr /user/wrt/sale_tmp
spark-submit  --executor-memory 9G  --driver-memory 10G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/t_base_item_sale.py $qiantian $zuotian 20160424
sh $pre_path/wrt/data_base_process/t_base_item_sale.sql $zuotian




#zuotian='20160333'
#qiantian='20160332'
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20160407/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1




#zuotian='20160318'
#qiantian='20160225'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1

#zuotian='20160319'
#qiantian='20160318'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160320'
#qiantian='20160319'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160323'
#qiantian='20160320'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160324'
#qiantian='20160323'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160325'
#qiantian='20160324'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160328'
#qiantian='20160325'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160329'
#qiantian='20160328'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1
#
#
#zuotian='20160330'
#qiantian='20160329'
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1

#zuotian='20160332'
#qiantian='20160331'

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /user/wrt/item_info_new/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1

#hadoop fs -rm -r /hive/warehouse/wlbase_dev.db/t_base_ec_item_dev/ds=$qiantian >> ./log_date/log_$zuotian 2>&1

#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160121'
#qiantian='20160120'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160122'
#qiantian='20160121'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1

#zuotian='20160123'
#qiantian='20160122'
#
##spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
##$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
##/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
##
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160124'
#qiantian='20160123'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160125'
#qiantian='20160124'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160126'
#qiantian='20160125'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160127'
#qiantian='20160126'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160128'
#qiantian='20160127'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160129'
#qiantian='20160128'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#zuotian='20160130'
#qiantian='20160129'
#
#spark-submit  --total-executor-cores  120  --executor-memory 12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/$zuotian/*  $qiantian $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 20g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/$zuotian/* $qiantian $zuotian \
#>> ./log_date/log_$zuotian 2>&1
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#hadoop fs -rm -r /user/wrt/sale_tmp >> ./log_date/log_$zuotian 2>&1
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale_new.py $qiantian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql $zuotian >> ./log_date/log_$zuotian 2>&1
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $qiantian $zuotian $zuotian $zuotian >> ./log_date/log_$zuotian 2>&1
