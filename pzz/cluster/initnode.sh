source ~/.bashrc

total_disk=$1

for i in `seq ${total_disk}`
do
	cmd="mkdir /mnt/hdfs/data${i}/yarn.hadoop_data;mkdir /mnt/hdfs/data${i}/yarn.yarn_tmp;mkdir /mnt/hdfs/data${i}/yarn.spark_tmp"
	echo $cmd
	#$cmd
	#cmd_hive="ALTER TABLE wlbase_dev.${table} add PARTITION (ds='${i}');${cmd_hive}"
done

#chown -R yarn:yarn /mnt/hdfs/data*

