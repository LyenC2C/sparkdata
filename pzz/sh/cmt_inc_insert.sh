source  /home/hadoop/.bashrc

#eg: sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh /data/develop/ec/tb/cmt_dbfeedid.20151103/part* /commit/comments/*2015-11-05 /data/develop/ec/tb/cmt_allfeedid.20151105 /data/develop/ec/tb/cmt_newfeedid.20151105 /data/develop/ec/tb/cmt_tmpdata.20151105

#获取输入参数
all_feed_input=$1
new_data_input=$2
all_feed_output=$3
new_feed_output=$4
tmp_data=$5

#处理数据
hadoop fs -rmr $all_feed_output
hadoop fs -rmr $new_feed_output
hadoop fs -rmr $tmp_data
spark-submit --executor-memory 20g --driver-memory 20g --total-executor-cores 105 /mnt/pzz/workspace/sparkdata/pzz/cmt/cmt_inc_clean.py -gen_data_inc $all_feed_input $new_data_input $all_feed_output $new_feed_output $tmp_data


#hive 入库
echo "cp result data for test.."$tmp_data" to "${tmp_data}.test
hadoop fs -rmr ${tmp_data}.test
hadoop fs -cp $tmp_data ${tmp_data}.test

echo "insert hive"
sh  /mnt/pzz/workspace/sparkdata/pzz/sh/feed.Dynamic_partitions.sql ${tmp_data}.test

echo "completed insertting "$tmp_data

#反馈商品评论增量
echo "feed back item feed inc number to commit.."
hadoop fs -cp $new_feed_output /commit_feedbck/cmt/

echo "mission FINISH!"