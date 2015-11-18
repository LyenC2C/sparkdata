pre_path='/home/wrt/sparkdata'


spark-submit  --executor-memory 4G  --driver-memory 20G  --total-executor-cores 80 t_wrt_base_ec_item_sale.py
sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151113 20151115