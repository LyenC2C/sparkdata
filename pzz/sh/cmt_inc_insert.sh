source  /home/hadoop/.bashrc

#eg: sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh /data/develop/ec/tb/cmt_dbfeedid.20151103/part* /commit/comments/*2015-11-05 /data/develop/ec/tb/cmt_allfeedid.20151105 /data/develop/ec/tb/cmt_newfeedid.20151105 /data/develop/ec/tb/cmt_tmpdata.20151105

feed_dir=/data/develop/ec/tb/cmt/feedid
data_dir=/data/develop/ec/tb/cmt/tmpdata
commit_dir=/commit/comments

#输入数据
mission_data=$1
#任务id
mission_id=$2
#上次任务id
lastmission_id=$3

#获取输入参数
all_feed_input=${feed_dir}/cmt_allfeedid.${lastmission_id}
new_data_input=$1
all_feed_output=${feed_dir}/cmt_allfeedid.${mission_id}
new_feed_output=${feed_dir}/cmt_newfeedid.${mission_id}
tmp_data=${data_dir}/cmt_inc_data.${mission_id}

start_t=`date`
echo "Mission start at:"${start_t}
echo "check argvs: "
echo $'\t'"data location:"$'\t'${new_data_input}
echo $'\t'"last all feedid location:"$'\t'${all_feed_input}
echo $'\t'"all feedid location:"$'\t'${all_feed_output}
echo $'\t'"new feedid location:"$'\t'${new_feed_output}

#处理数据
echo "Start spark job."
hadoop fs -rmr $all_feed_output
hadoop fs -rmr $new_feed_output
hadoop fs -rmr $tmp_data
spark-submit --executor-memory 8g --driver-memory 25g --total-executor-cores 80 /mnt/pzz/workspace/sparkdata/pzz/cmt/cmt_inc_clean.py -gen_data_inc ${all_feed_input}/part* $new_data_input $all_feed_output $new_feed_output $tmp_data
echo "spark job finished."

#本地临时文件
local_tmp_new_feed=/mnt/pzz/hdfs_merge_tmp/cmt_newfeedid.${mission_id}.partall
local_tmp_inc_data=/mnt/pzz/hdfs_merge_tmp/cmt_inc_data.${mission_id}.partall

#hive 入库
echo "cat and put result data  dir.."$tmp_data" to "${tmp_data}.test
hadoop fs -cat ${new_feed_output}/part* > ${local_tmp_new_feed}
hadoop fs -rmr ${new_feed_output}/part*
hadoop fs -put ${local_tmp_new_feed} ${new_feed_output}/

hadoop fs -cat ${tmp_data}/part* > ${local_tmp_inc_data}
hadoop fs -rmr ${tmp_data}/part*
hadoop fs -put ${local_tmp_inc_data} ${tmp_data}/

hadoop fs -rmr ${tmp_data}.test
hadoop fs -cp $tmp_data ${tmp_data}.test

echo "insert hive"
sh  /mnt/pzz/workspace/sparkdata/pzz/sh/feed.Dynamic_partitions.sql ${tmp_data}.test

echo "completed insertting "$tmp_data

#反馈商品评论增量
echo "feed back item feed inc number to commit.."
hadoop fs -rmr /commit_feedbck/cmt/cmt_newfeedid.${mission_id}
hadoop fs -cp $new_feed_output /commit_feedbck/cmt/

echo "mission FINISH! "$2
end_t=`date`
echo "start at:"${start_t}", end at:"${end_t}
echo $2 >> /mnt/pzz/workspace/sparkdata/mission_finished
