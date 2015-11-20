pre_path='/home/wrt/wrt/sparkdata'

spark-submit  --executor-memory 4G  --driver-memory 20G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py /commit/shopitem/20151117

sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151113 20151115

spark-submit  --total-executor-cores  100   --executor-memory  20g  --driver-memory 20g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-02  20151101 20151102

spark-submit  --total-executor-cores  100  --executor-memory 20g  --driver-memory 20g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01  20151030 20151101
