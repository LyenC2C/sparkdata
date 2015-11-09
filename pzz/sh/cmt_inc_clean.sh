source  /home/hadoop/.bashrc
#spark-submit --executor-memory 20g --driver-memory 20g --total-executor-cores 120 pzz/cmt/cmt_inc_clean.py -gen_his_item_feed_file /hive/warehouse/wlbase_dev.db/t_base_ec_item_feed_dev/ds=201*/* /data/develop/ec/tb/cmt_dbfeedid.20151103^C

all_feed_output=/data/develop/ec/tb/cmt_allfeedid.20151105
new_feed_output=/data/develop/ec/tb/cmt_newfeedid.20151105
tmp_data=/data/develop/ec/tb/cmt_tmpdata.20151105

#hadoop fs -rmr $all_feed_output
#hadoop fs -rmr $new_feed_output
#hadoop fs -rmr $tmp_data
#spark-submit --executor-memory 20g --driver-memory 20g --total-executor-cores 100 /mnt/pzz/workspace/sparkdata/pzz/cmt/cmt_inc_clean.py -gen_data_inc /data/develop/ec/tb/cmt_dbfeedid.20151103/part* /commit/comments/*2015-11-05 $all_feed_output $new_feed_output $tmp_data

sh  /mnt/pzz/workspace/sparkdata/pzz/sh/feed.Dynamic_partitions.sql ${tmp_data}.test