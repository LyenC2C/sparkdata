spark-submit  --total-executor-cores  100   --executor-memory  20g --driver-memory 20g  1_item_inc.py  -inc $1 $2 $3
