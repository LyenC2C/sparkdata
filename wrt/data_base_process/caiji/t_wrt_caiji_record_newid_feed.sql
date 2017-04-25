drop table wl_analysis.t_wrt_caiji_record_newid_feed;
create table wl_analysis.t_wrt_caiji_record_newid_feed(
item_id string comment '商品id'
)
COMMENT '采集评论所需的新晋商品id'
PARTITIONED by (ds string)
location '/commit/ids_4_crawler/shopitem_newid_feed';
--新晋商品且销量不超过5000和1000

insert overwrite table wl_analysis.t_wrt_caiji_record_newid_feed partition(ds = '20170425')
select item_id from
(
select
t1.item_id,
t1.sold
from
(
select item_id,sold from wl_base.t_base_ec_shopitem_c where ds = '20170422' and sold <= 5000
union all
select item_id,sold from wl_base.t_base_ec_shopitem_b where ds = '20170424' and sold <= 1000
)t1
left join
(select item_id from wl_base.t_base_ec_record_dev_new where ds = 'true' group by item_id)tt2
on
t1.item_id = cast(tt2.item_id as string)
WHERE
tt2.item_id is null
)t
order by sold desc;


