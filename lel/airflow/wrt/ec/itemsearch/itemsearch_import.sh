#!/usr/bin/env bash
source ~/.bashrc

lastday=$1
last_update_date=$2

table=wl_base.t_base_item_search

hive<<EOF
use wlbase_dev;
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