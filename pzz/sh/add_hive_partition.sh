source /home/yarn/.bashrc

table=$1
start=$2
end=$3


for((i=${start};i<=${end};i++))
do
	#cmd="hadoop fs -mkdir /hive/warehouse/wlbase_dev.db/${table}/ds=${i}"
	#echo $cmd
	#$cmd
	#cmd_hive="ALTER TABLE wlbase_dev.${table} add PARTITION (ds='${i}');${cmd_hive}"
	cmd_tmp="ALTER TABLE t_base_ec_item_feed_dev DROP IF EXISTS PARTITION (ds='${i}');ALTER TABLE t_base_ec_item_feed_dev ADD partition(ds='${i}');${cmd_hive}"
done

#hive -e $cmd_hive
#hive -e "show tables;show tables"
echo $cmd_hive
echo "need executor this script with hive -e \"ALTER xxxxxxx....\""
