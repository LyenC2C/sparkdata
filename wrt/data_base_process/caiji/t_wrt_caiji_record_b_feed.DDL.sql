drop table wl_analysis.t_wrt_caiji_record_b_feed
create table wl_analysis.t_wrt_caiji_record_b_feed(
item_id string comment '商品id',
new_ds string comment '商品在已在库中的最新时间',
sold string comment '销量(采集次序需要)'
)
COMMENT '采集天猫评论所需的商品id';

insert overwrite table wl_analysis.t_wrt_caiji_record_b_feed
select item_id,new_ds,sold from
(
select
t1.item_id,
case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds,
t1.sold
from
(select item_id,sold from wlbase_dev.t_base_ec_shopitem_b where ds = 20170116)t1
left join
(select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'B'
group by item_id)t2
on
t1.item_id = t2.item_id
)t
order by sold desc;