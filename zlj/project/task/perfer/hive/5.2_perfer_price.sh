



hive -f  5.1_perfer_price.sql

spark-submit   --executor-memory 20G   --num-executors 80   ../spakr/user_perfer_price.py