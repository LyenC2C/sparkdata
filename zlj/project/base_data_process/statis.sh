
#zlj


#有ds的数据统计最新ds
#表基本信息统计


date_now =`date +%Y%m%d`

echo $date_now >> file
echo '---------\n\n' >>file


dstables=("t_base_ec_item_dev_new" "t_base_ec_shop_dev_new")


for var in ${dstables[@]};
do
    echo $var
    table=$var
    ds=`hadoop fs -ls /hive/warehouse/wlbase_dev.db/$table  |tail -1 |awk -F'=' '{print $2}'`
    data=`hive -e "use wlbase_dev;  select count(1) from $table where ds=$ds  " |tail -2`
    echo $table ,$ds ,$data  >>file 
done


tables=("t_base_ec_item_feed_dev" )

for var in ${tables[@]};
do
    echo $var
    table=$var
    data=`hive -e "use wlbase_dev;  select count(1) from $table   " |tail -2`
    echo $table ,$data >>file
done

table='t_base_ec_record_dev_new'
data=`hive -e "use wlbase_dev;  select count(1) from $table  where ds=true1  " |tail -2`
echo $table   , $data >>file


table='t_base_ec_item_sold_dev'
data=`hive -e "use wlbase_dev;  select count(1) from $table  and cp_flag <>'1'  " |tail -2`
echo $table   ,$data >>file