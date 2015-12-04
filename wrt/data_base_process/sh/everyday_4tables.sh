pre_path='/home/wrt/wrt/sparkdata'
today=$(date +%Y%m%d)
zuotian=$(date -d '1 days ago' +%Y%m%d)
qiantian=$(date -d '2 days ago' +%Y%m%d)

spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py $zuotian $qiantian

spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/$zuotian/*  $qiantian $zuotian

spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/$zuotian/*  $qiantian $zuotian

sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql $zuotian $qiantian $zuotian $zuotian