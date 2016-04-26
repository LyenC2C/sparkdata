source /home/yarn/.bashrc
if [ $1 = "-h" ];then
echo "arg1: -byXXX arg2:XX inutpath arg3:outputpath"
else

local_tmp_refer=/mnt/raid1/pzz/hdfs_merge_tmp/record.tmp

spark_middle /home/yarn/pzz/workspace/extract_code/taobao/record/extract_record_from_hivedb_spark.py $1 $2 $3".tmpdir"
hadoop fs -cat $3".tmpdir"/part* > ${local_tmp_refer}
hadoop fs -rmr $3".tmpdir"
hadoop fs -put ${local_tmp_refer} $3
fi