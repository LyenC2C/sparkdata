drop table wl_analysis.t_wrt_caiji_record_highsold_feed;
create table wl_analysis.t_wrt_caiji_record_highsold_feed(
item_id string comment '商品id',
new_ds string comment '商品在已在库中的最新时间'
)
COMMENT '采集评论所需的高销量商品id'
PARTITIONED by (ds string)
location '/commit/ids_4_crawler/shopitem_highsold_feed';


insert overwrite table wl_analysis.t_wrt_caiji_record_highsold_feed partition(ds = '20170425')
select
t1.item_id,
case when t2.new_ds is null then "19760101" else t2.new_ds end as new_ds
from
(select item_id from wl_base.t_base_ec_shopitem_c where ds = '20170422' and sold > 1000
union all
select item_id from wl_base.t_base_ec_shopitem_b where ds = '20170424' and sold > 5000)t1
left join
(select item_id,max(dsn) as new_ds from wl_base.t_base_ec_record_dev_new where ds = 'true'
group by item_id)t2
on
cast(t1.item_id as bigint) = t2.item_id;


