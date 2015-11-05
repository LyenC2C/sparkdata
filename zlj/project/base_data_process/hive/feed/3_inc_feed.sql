


#!/bin/bash

source /etc/profile;

m=2
m_1=`expr $m + 1`
d_1=$(date -d '-'$m' day' '+%Y%m%d')
d_2=$(date -d '-'$m_1' day' '+%Y%m%d')



lastmonth=$(date -d '-1 month' '+%Y%m%d')
path=$2



##hive  ²¿·Ö

/home/zlj/hive/bin/hive<<EOF


use wlbase_dev;

insert overwrite table t_base_ec_item_feed_inc PARTITION(ds='$d_1')
SELECT
 t1.item_id,t1.feed_times- (case when  t2.feed_times is null then 0 else t2.feed_times end ) t
from
(
select * from t_base_ec_feed_add_everyday where ds=$d_1 and item_id rlike   '^\\d+$'
)t1
left join
(
select * from t_base_ec_feed_add_everyday where ds=$d_2 and item_id rlike   '^\\d+$'
)t2

on t1.item_id=t2.item_id
;


EOF