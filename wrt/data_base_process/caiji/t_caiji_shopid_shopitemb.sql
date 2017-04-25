create EXTERNAL table wl_analysis.t_wrt_caiji_shopid_shopitemb (
shop_id string comment 'shopid'
)
comment '采集shopitem_b所需shopid'
PARTITIONED BY  (ds STRING )
location '/commit/ids_4_crawler/t_caiji_shopid_shopitemb';

insert overwrite table wl_analysis.t_wrt_caiji_shopid_shopitemb partition(ds = '20170418')
select
case when t1.shop_id is null then t2.shop_id else t1.shop_id end as shop_id from
(select shop_id from wl_base.t_base_ec_shop_dev_new where ds = '20170415'and bc_type = 'B' group by shop_id)t1
full join
(select shop_id from wl_base.t_base_ec_shopitem_b where ds = '20170417' group by shop_id)t2
on
t1.shop_id = t2.shop_id;

