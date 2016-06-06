source  /home/yarn/.bashrc

#eg: sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh /data/develop/ec/tb/cmt_dbfeedid.20151103/part* /commit/comments/*2015-11-05 /data/develop/ec/tb/cmt_allfeedid.20151105 /data/develop/ec/tb/cmt_newfeedid.20151105 /data/develop/ec/tb/cmt_tmpdata.20151105

uid_feedls_dir=/data/develop/ec/tb/cmt/feedid
uid_markls_dir=/data/develop/ec/tb/cmt/uid_mark
data_dir=/data/develop/ec/tb/cmt/tmpdata
data_nouid_dir=/data/develop/ec/tb/cmt/tmpdata.nouid
user_dir=/data/develop/ec/tb/cmt/user
commit_dir=/commit/comments
feed_bck_dir=/commit_feedbck/cmt

#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

#table
table=t_base_ec_item_feed_dev

#任务id
mission_id=$1
#输入数据
mission_data="/commit/comments/"$1"/*"
#上次任务id
lastmission_id=$2

#获取输入参数
uid_feed_input=${uid_feedls_dir}/all_uid_mark_feedids.${lastmission_id}
uid_mark_input=${uid_markls_dir}/uid_mark_freq.json.${lastmission_id}
new_data_input=$mission_data

uid_feed_output=${uid_feedls_dir}/all_uid_mark_feedids.${mission_id}
uid_mark_output=${uid_markls_dir}/uid_mark_freq.json.${mission_id}

uid_data_output=${data_dir}/cmt_inc_data.uid.${mission_id}
nouid_data_output=${data_nouid_dir}/cmt_inc_data.nouid.${mission_id}
user_data=${user_dir}/user.${mission_id}
item_itc_num_output=${feed_bck_dir}/inc_item_num.${mission_id}

start_t=`date`
echo "Mission start at:"${start_t}
echo "check argvs: "
echo $'\t'"data location:"$'\t'${new_data_input}
echo $'\t'"last uid feedid location:"$'\t'${uid_feed_input}
echo $'\t'"last uid mark location:"$'\t'${uid_mark_input}
echo $'\t'"all feedid location:"$'\t'${uid_feed_output}
echo $'\t'"new cmt inc location:"$'\t'${uid_data_output}

#处理数据
echo "1/5 Start spark job."
hadoop fs -rmr ${uid_feed_output}
hadoop fs -rmr ${uid_mark_output}
hadoop fs -rmr ${uid_data_output}
hadoop fs -rmr ${nouid_data_output}

spark-submit --executor-memory 10g --driver-memory 20g --total-executor-cores 140 ${workspace_path}/pzz/cmt/cmt_inc_updated_0601.py\
 -gen_data_inc ${uid_feed_input} ${uid_mark_input} ${new_data_input} ${uid_data_output} ${nouid_data_output} ${uid_feed_output} ${uid_mark_output} ${item_itc_num_output}
echo "spark job finished."

#本地临时文件
local_tmp_item_inc_num=${workspace_path}/../../hdfs_merge_tmp/item_inc_num.${mission_id}.partall
local_tmp_inc_data=${workspace_path}/../../hdfs_merge_tmp/cmt_inc_data.${mission_id}.partall
local_nouid_data=${workspace_path}/../../hdfs_merge_tmp/local_nouid_data.${mission_id}.partall

#合并文件
echo "2/5 cat and put result data  dir.."$tmp_data
hadoop fs -cat ${item_itc_num_output}/part* > ${local_tmp_item_inc_num}
hadoop fs -rmr ${item_itc_num_output}/part*
hadoop fs -put ${local_tmp_item_inc_num} ${item_itc_num_output}/

hadoop fs -cat ${nouid_data_output}/part* > ${local_nouid_data}
hadoop fs -rmr ${nouid_data_output}/part*
hadoop fs -put ${local_nouid_data} ${nouid_data_output}/

hadoop fs -cat ${uid_data_output}/part* > ${local_tmp_inc_data}
hadoop fs -rmr ${uid_data_output}/part*
hadoop fs -put ${local_tmp_inc_data} ${uid_data_output}/
hadoop fs -chmod -R 775 ${uid_data_output}

#数据分区
echo "3/5 inc data partitions.."
hadoop fs -rmr ${tmp_data}.partitions
spark-submit  --master spark://cs220:7077  --total-executor-cores  40 --executor-memory  4g --driver-memory 4g --class MultipleText  ${workspace_path}/pzz/sh/partition_spark.jar  ${tmp_data} ${tmp_data}.partitions

#插入hive
echo "4/5 mv partitions to hive.."
sh ${workspace_path}/pzz/sh/mv_feed_from_partitions.sh ${tmp_data}.partitions ${table}

#echo "insert hive"
#sh ${workspace_path}/pzz/sh/feed.Dynamic_partitions.sql ${tmp_data}.test

#反馈商品评论增量
echo "5/5 feed back item feed inc number to commit.."
hadoop fs -rmr /commit_feedbck/cmt/inc_item_num.${mission_id}
hadoop fs -cp $new_feed_output /commit_feedbck/cmt/

echo "mission FINISH! "$1
end_t=`date`
echo "start at:"${start_t}", end at:"${end_t}
echo $1 >> ${workspace_path}/pzz/cmt/mission_finished
