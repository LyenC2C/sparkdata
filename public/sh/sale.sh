pre_path='/home/wrt/wrt/sparkdata'

spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151117 20151116

sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151113 20151115

spark-submit  --total-executor-cores  40   --executor-memory  8g  --driver-memory 8g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/20151117/*  20151112 20151117
#/commit/iteminfo
spark-submit  --total-executor-cores  100  --executor-memory 10g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/20151117/*  20151112 20151117
