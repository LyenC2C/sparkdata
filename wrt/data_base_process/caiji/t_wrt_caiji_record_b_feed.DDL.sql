drop table wl_analysis.t_wrt_caiji_record_b_feed;
create EXTERNAL table wl_analysis.t_wrt_caiji_record_b_feed(
item_id string comment '商品id',
new_ds string comment '商品在已在库中的最新时间',
sold bigint comment '销量(采集次序需要)'
)
COMMENT '采集天猫评论所需的商品id'
PARTITIONED by (ds string)
location '/commit/ids_4_crawler/shopitem_b_feed' ;


insert overwrite table wl_analysis.t_wrt_caiji_record_b_feed partition(ds = '20170420')
select item_id,new_ds,sold from
(
select
t1.item_id,
case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds,
t1.sold
from
(select item_id,sold from wl_base.t_base_ec_shopitem_b where ds = '20170419')t1
left join
(select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new
where ds = 'true' and bc_type = 'B'
group by item_id)t2
on
t1.item_id = cast(t2.item_id as string)
)t
order by sold desc;

-- insert overwrite table wl_analysis.t_wrt_caiji_record_b_feed partition(ds = '20170420_highsold')
-- select item_id,new_ds,sold from
-- (
-- select
-- t1.item_id,
-- case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds,
-- t1.sold
-- from
-- (select item_id,sold from wl_base.t_base_ec_shopitem_b where ds = '20170419')t1
-- left join
-- (select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'B'
-- group by item_id)t2
-- on
-- t1.item_id = cast(t2.item_id as string)
-- WHERE
-- t1.sold > 100000
-- )t
-- order by sold desc;




