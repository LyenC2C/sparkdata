#!/usr/bin

#闲鱼itemcomment——日更——当天处理的是昨天的数据
source ~/.bashrc
date
date  +%Y%m%d

lastday=$(date -d '1 days ago' +%Y%m%d)
last_2_days=$(date -d '2 days ago' +%Y%m%d)

table=wlbase_dev.t_base_ec_xianyu_itemcomment

hive<<EOF
use wlbase_dev;
LOAD DATA  INPATH '/user/lel/temp/xianyu_itemcomment' OVERWRITE INTO TABLE $table PARTITION (ds='0000tmp');
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
(select * from  $table where ds = '0000tmp')t1
full outer JOIN
(select * from $table where ds = $last_2_days)t2
ON
t1.commentId = t2.commentId;
EOF
