source  /home/yarn/.bashrc

#eg: sh /mnt/pzz/workspace/sparkdata/pzz/sh/cmt_inc_insert.sh /data/develop/ec/tb/cmt_dbfeedid.20151103/part* /commit/comments/*2015-11-05 /data/develop/ec/tb/cmt_allfeedid.20151105 /data/develop/ec/tb/cmt_newfeedid.20151105 /data/develop/ec/tb/cmt_tmpdata.20151105

spark-submit --executor-memory 20g --driver-memory 10g --total-executor-cores 200 \
/mnt/raid1/pzz/workspace/sparkdata/pzz/cmt/cmt_inc_updated_1128.py \
-gen_cmt_inc \
/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161217 \
/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20161210 \
/commit/comments/2016121[8-9]/* \
/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161219 \
/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.20161219 \
/data/develop/ec/tb/cmt/tmpdata.nouid/cmt_inc_data.nouid.20161219 \
/data/develop/ec/tb/cmt/user/user.20161219

itemid_feedid_dir=/data/develop/ec/tb/cmt/itemid_feedid
uid_markls_dir=/data/develop/ec/tb/cmt/uid_mark
commit_dir=/commit/comments
data_dir=/data/develop/ec/tb/cmt/tmpdata
data_nouid_dir=/data/develop/ec/tb/cmt/tmpdata.nouid
user_dir=/data/develop/ec/tb/cmt/user

/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161031 \
/data/develop/ec/tb/cmt/uid_mark/uid_mark_freq.json.20161129 \
/commit/comments/201611*/* \
/data/develop/ec/tb/cmt/itemid_feedid/itemid_feediduidls.20161130 \
/data/develop/ec/tb/cmt/tmpdata/cmt_inc_data.uid.20161130 \
/data/develop/ec/tb/cmt/tmpdata.nouid/cmt_inc_data.nouid.20161130 \
/data/develop/ec/tb/cmt/user/user.20161130



#workspace path
workspace_path=/mnt/raid1/pzz/workspace/sparkdata

#table
table=t_base_ec_item_feed_dev_new

#输入数据
mission_data=$1
#任务id
mission_id=$2
#上次任务id
lastmission_id=$3

hadoop fs -mv /commit/comments/tmp/* /commit/comments/
hadoop fs -mv ${mission_data} /commit/comments/tmp/

#获取输入参数
uid_feed_input=${uid_feedls_dir}/all_uid_mark_feedids.${lastmission_id}
uid_mark_input=${uid_markls_dir}/uid_mark_freq.json.${lastmission_id}
new_data_input=/commit/comments/tmp/*/*

uid_feed_output=${uid_feedls_dir}/all_uid_mark_feedids.${mission_id}

uid_mark_output=${uid_markls_dir}/uid_mark_freq.json.${mission_id}

uid_data_output=${data_dir}/cmt_inc_data.uid.${mission_id}
nouid_data_output=${data_nouid_dir}/cmt_inc_data.nouid.${mission_id}
user_data=${user_dir}/user.${mission_id}

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
hadoop fs -rmr ${item_itc_num_output}
hadoop fs -rmr ${user_data}

spark-submit --executor-memory 16g --driver-memory 20g --total-executor-cores 180 ${workspace_path}/pzz/cmt/cmt_inc_updated_0824.py\
 -gen_data_inc ${uid_feed_input} ${uid_mark_input} ${new_data_input} ${uid_data_output} ${nouid_data_output} ${uid_feed_output} ${uid_mark_output} ${item_itc_num_output} ${user_data}
echo "spark job finished."

#本地临时文件
local_tmp_item_inc_num=${workspace_path}/../../hdfs_merge_tmp/item_inc_num.${mission_id}.partall
local_tmp_inc_data=${workspace_path}/../../hdfs_merge_tmp/cmt_inc_data.uid.${mission_id}.partall
local_nouid_data=${workspace_path}/../../hdfs_merge_tmp/cmt_inc_data.nouid.${mission_id}.partall

#合并文件
echo "2/5 cat and put result data  dir.."${uid_data_output}
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
hadoop fs -rmr ${uid_data_output}.partitions
spark-submit  --master spark://cs220:7077  --total-executor-cores  40 --executor-memory  4g --driver-memory 4g --class MultipleText  ${workspace_path}/pzz/sh/partition_spark.jar  ${uid_data_output} ${uid_data_output}.partitions

#插入hive
echo "4/5 mv partitions to hive.."
sh ${workspace_path}/pzz/sh/mv_feed_from_partitions.sh ${uid_data_output}.partitions ${table}


echo "mission FINISH! "$1
end_t=`date`
echo "start at:"${start_t}", end at:"${end_t}
echo $1 >> ${workspace_path}/pzz/cmt/mission_finished
