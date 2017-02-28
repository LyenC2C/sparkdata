#!/usr/bin/env bash
source ~/.bashrc

last_update_date=$1

hive<<EOF
insert overwrite table wl_analysis.t_wrt_caiji_record_c_feed
select item_id,new_ds,sold from
(
select
t1.item_id,
case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds,
t1.sold
from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_c where ds = $last_update_date)t1
left join
(select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'C'
group by item_id)t2
on
t1.item_id = t2.item_id
)t
order by sold desc;
EOF