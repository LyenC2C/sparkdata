#!/usr/bin/env bash

hive<<EOF
insert overwrite table wl_analysis.t_wrt_caiji_shopitem_c_newid
select t1.item_id,t1.sold from
(select item_id,cast(sold as int) as sold from wlbase_dev.t_base_ec_shopitem_c where ds = '20170220')t1
left join
(select item_id from wl_base.t_base_ec_item_dev_new where ds = '20170217')t2
on
t1.item_id = t2.item_id
where
t2.item_id is null
order by t1.sold desc;
EOF
