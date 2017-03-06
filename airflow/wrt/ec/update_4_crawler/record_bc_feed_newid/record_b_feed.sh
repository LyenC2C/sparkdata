#!/usr/bin/env bash
source ~/.bashrc

last_update_date=$1

hive<<EOF
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
insert overwrite table wl_analysis.t_wrt_caiji_record_b_feed partition(ds='20170302')
select item_id,new_ds,sold from
(
select
t1.item_id,
case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds,
t1.sold
from
(select item_id,sold from wl_base.t_base_ec_shopitem_b where ds = 20170301 and cast(sold as int) > 0)t1
left join
(select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'B'
group by item_id)t2
on
t1.item_id = t2.item_id
)t
order by sold desc;
EOF
