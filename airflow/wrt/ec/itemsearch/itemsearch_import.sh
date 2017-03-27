#!/usr/bin/env bash
source ~/.bashrc

lastday=$1
last_update_date=$2

table=wl_base.t_base_item_search
database=wl_base
db_path=$database.db
total_size=`hadoop fs -du -s  /hive/warehouse/$db_path/$table/ds=$last_update_date | awk '{print $1/1024/1024}'`
dynamic_reducers=`awk 'BEGIN{print int(('$total_size'/256)+0.5)+5}'`

hadoop fs -test -e  /hive/warehouse/$db_path/$table/ds=$lastday
if [ $? -eq 0 ] ;then
    hadoop fs  -rmr /hive/warehouse/$db_path/$table/ds=$lastday
else
    echo 'Partition not exist,you can run hive work as you want!!!'
fi

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
set hive.merge.mapfiles= true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=192000000;
insert OVERWRITE table $table PARTITION(ds = $lastday)
select
case when t1.nid is null then t2.nid else t1.nid end,
case when t1.nid is null then t2.user_id else t1.user_id end,
case when t1.nid is null then t2.comment_count else t1.comment_count end,
case when t1.nid is null then t2.encryptedUserId else t1.encryptedUserId end,
case when t1.nid is null then t2.nick else t1.nick end
from
(select * from  $table where ds = '0temp')t1
full outer JOIN
(select * from $table where ds = $last_update_date)t2
ON
t1.nid = t2.nid;
EOF