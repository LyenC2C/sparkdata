source  /home/hadoop/.bashrc

#eg: sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_clean.sh /data/develop/ec/tb/cmt_dbfeedid.20151103/part* /commit/comments/*2015-11-05 /data/develop/ec/tb/cmt_allfeedid.20151105 /data/develop/ec/tb/cmt_newfeedid.20151103 /data/develop/ec/tb/cmt_tmpdata.20151103

#all_feed_output=/data/develop/ec/tb/cmt_allfeedid.20151105
#new_feed_output=/data/develop/ec/tb/cmt_newfeedid.20151105
#tmp_data=/data/develop/ec/tb/cmt_tmpdata.20151105

all_feed_input=$1
#/data/develop/ec/tb/cmt_dbfeedid.20151103/part*
new_data_input=$2
#/commit/comments/*2015-11-05
all_feed_output=$3
new_feed_output=$4
tmp_data=$5

hadoop fs -rmr $all_feed_output
hadoop fs -rmr $new_feed_output
hadoop fs -rmr $tmp_data

echo 'cp result data for test..'
hadoop fs -cp $tmp_data ${tmp_data}.test

spark-submit --executor-memory 20g --driver-memory 20g --total-executor-cores 100 /mnt/pzz/workspace/sparkdata/pzz/cmt/cmt_inc_clean.py -gen_data_inc $all_feed_input $new_data_input $all_feed_output $new_feed_output $tmp_data


echo 'cp result data for test..'
hadoop fs -cp $tmp_data ${tmp_data}.test

echo 'insert hive'
sh  /mnt/pzz/workspace/sparkdata/pzz/sh/feed.Dynamic_partitions.sql ${tmp_data}.test