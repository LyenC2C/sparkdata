#spark-submit  --total-executor-cores  100   --executor-memory  20g --driver-memory 20g  1_item_inc.py  -inc $1 $2 $3


hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 10g 1_item_inc_opt.py  -inc /commit/iteminfo/$2/*  $1 $2
sh 1_item_inc.sql $2