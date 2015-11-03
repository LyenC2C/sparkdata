source /home/zlj/.bashrc
hadoop fs  -rm -r "/hive/external/wlbase_dev/t_base_ec_item_dev/ds=20150101"

spark-submit  --executor-memory 4G  --driver-memory 20G  --total-executor-cores 80  /home/zlj/workspace/sparkJobs/project/base_data_process/item.py  $1

hive -f /home/zlj/workspace/sparkJobs/project/base_data_process/spark2hive/t_base_ec_item_dev.sql
