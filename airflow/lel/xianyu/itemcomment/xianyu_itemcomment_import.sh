#!/usr/bin

source ~/.bashrc
date
date  +%Y%m%d

lastday=$1
last_update_date=$2
table=t_base_ec_xianyu_itemcomment
database=wl_base
db_path=$database.db
total_size=`hadoop fs -du -s  /hive/warehouse/$db_path/$table/ds=$lastday | awk '{print $1/1024/1024}'`
offset=5
dynamic_reducers=`awk 'BEGIN{print int(('$total_size'/256)+0.5)+5}'`

hive<<EOF
use $database;
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
LOAD DATA  INPATH '/user/lel/temp/xianyu_itemcomment' OVERWRITE INTO TABLE $table PARTITION (ds='00tmp');
insert OVERWRITE table $table PARTITION(ds = $lastday)
select
case when t1.commentId is null then t2.itemid else t1.itemid end,
case when t1.commentId is null then t2.commentId else t1.commentId end,
case when t1.commentId is null then t2.content else t1.content end,
case when t1.commentId is null then t2.reportTime else t1.reportTime end,
case when t1.commentId is null then t2.reporterName else t1.reporterName end,
case when t1.commentId is null then t2.reporterNick else t1.reporterNick end,
case when t1.commentId is null then t2.ts else t1.ts end
from
(select * from  $table where ds = '00tmp')t1
full outer JOIN
(select * from $table where ds = $last_update_date)t2
ON
t1.commentId = t2.commentId;
EOF
