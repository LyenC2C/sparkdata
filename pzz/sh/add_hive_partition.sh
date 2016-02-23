source /home/yarn/.bashrc

start=20160209
end=20160228
table="t_base_ec_item_feed_dev_test"

for((i=${start};i<=${end};i++))
do
	cmd="hadoop fs -mkdir /hive/warehouse/wlbase_dev.db/${table}/ds=${i}"
	echo $cmd
	$cmd
	cmd_hive="ALTER TABLE wlbase_dev.${table} add PARTITION (ds='${i}');${cmd_hive}"
done

#hive -e $cmd_hive
#hive -e "show tables;show tables"
echo $cmd_hive
echo "need executor this script with hive -e \"ALTER xxxxxxx....\""
