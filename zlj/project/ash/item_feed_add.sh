#!/bin/sh
#删除上次解析的数据，然后spark json处理 。最后hive去重后导入

hfs -rm -r /hive/external/wlbase_dev/t_base_ec_item_tag_dev/ds=20000103

spark-submit   --executor-memory 4G  --driver-memory 20G  --total-executor-cores 40  /home/zlj/workspace/sparkJobs/project/base_data_process/cmt_new.py  $1

hive -f /home/zlj/workspace/sparkJobs/project/datahive/data/t_base_ec_item_feed_dev.Dynamic_partition_new.sql

