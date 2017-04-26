drop table wl_analysis.t_wrt_caiji_record_lowsold_feed;
create table wl_analysis.t_wrt_caiji_record_lowsold_feed(
item_id string comment '商品id',
new_ds string comment '商品在已在库中的最新时间',
sold bigint comment '销量(采集次序需要)'
)
COMMENT '采集评论所需的低销量商品id'
PARTITIONED by (ds string)
location '/commit/ids_4_crawler/shopitem_lowsold_feed' ;



insert overwrite table wl_analysis.t_wrt_caiji_record_lowsold_feed partition(ds = '20170425')
select item_id,new_ds,sold from
(
select
t1.item_id,
t2.new_ds,
t1.sold
from
(
select item_id,sold from wl_base.t_base_ec_shopitem_b where ds = '20170424' and sold <= 5000
union ALL
select item_id,sold from wl_base.t_base_ec_shopitem_c where ds = '20170422' and sold <= 1000
)t1
join
(select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'C'
group by item_id)t2
on
cast(t1.item_id as bigint) = t2.item_id
)t
order by sold desc;
-- insert overwrite table wl_analysis.t_wrt_caiji_record_c_feed partition(ds = '20170420_highsold')
-- select item_id,new_ds,sold from
-- (
-- select
-- t1.item_id,
-- case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds,
-- t1.sold
-- from
-- (select item_id,sold from wl_base.t_base_ec_shopitem_c where ds = '20170419')t1
-- left join
-- (select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true' and bc_type = 'C'
-- group by item_id)t2
-- on
-- t1.item_id = cast(t2.item_id as string)
-- WHERE
-- t1.sold > 10000
-- )t
-- order by sold desc;