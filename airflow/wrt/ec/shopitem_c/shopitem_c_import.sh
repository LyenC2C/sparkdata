#!/usr/bin/env bash
source ~/.bashrc

lastday=$1
last_update_day=$2

table=t_base_ec_shopitem_c
database=wl_base
db_path=$database.db
estimate_date=`hadoop fs -ls /hive/warehouse/$db_path/$table | awk -F '=' '{if($2 ~ /^[0-9]+$/)print $2}' | sort -r |awk 'NR==1{print $0}'`
total_size=`hadoop fs -du -s  /hive/warehouse/$db_path/$table/ds=$estimate_date | awk '{print $1/1024/1024}'`
offset=5
dynamic_reducers=`awk 'BEGIN{print int(('$total_size'/256)+0.5)+5}'`
hadoop fs -test -e  /hive/warehouse/$db_path/$table/ds=$lastday
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /hive/warehouse/$db_path/$table/ds=$lastday
else
    echo 'Partition not exist,you can run your hive work as you want!!!'
fi

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
use wl_base;
set mapred.max.split.size=268435456;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.min.split.size.per.node=201326592;
set mapred.min.split.size.per.rack=201326592;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=201326592;
set hive.exec.reducers.bytes.per.reducer=268435456;
set hive.exec.reducers.max=1099;
set mapreduce.job.reduces=$dynamic_reducers;
LOAD DATA  INPATH '/user/wrt/shopitem_c_tmp' OVERWRITE INTO TABLE t_base_ec_shopitem_c PARTITION (ds='00tmp');
insert OVERWRITE table $table PARTITION(ds = $lastday)
select
case when t1.item_id is null then t2.shop_id else t1.shop_id end,
case when t1.item_id is null then t2.item_id else t1.item_id end,
case when t1.item_id is null then t2.sold else t1.sold end,
case when t1.item_id is null then t2.saleprice else t1.saleprice  end,
case when t2.item_id is null then t1.up_day else t2.up_day end,
case when t1.item_id is null then t2.update_day else t1.update_day end,
case when t1.item_id is null then t2.ts else t1.ts end
from
(select * from $table where ds = '00tmp')t1
full outer join
(select * from $table where ds = $last_update_day)t2
on
t1.item_id = t2.item_id;
EOF
