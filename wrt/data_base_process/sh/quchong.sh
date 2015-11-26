pre_path='/home/wrt/wrt/sparkdata'

spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/20151121/*  20151120 20151121
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/20151122/*  20151121 20151122
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/20151123/*  20151122 20151123
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/20151124/*  20151123 20151124
spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
/commit/iteminfo/20151125/*  20151124 20151125

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
