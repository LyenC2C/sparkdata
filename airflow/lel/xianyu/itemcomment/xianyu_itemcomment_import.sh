#!/usr/bin

source ~/.bashrc
date
date  +%Y%m%d

lastday=$1
last_update_date=$2
table=wl_base.t_base_ec_xianyu_itemcomment

hive<<EOF
use wlbase_dev;
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
