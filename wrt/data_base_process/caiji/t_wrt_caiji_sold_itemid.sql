
create table wl_analysis.t_wrt_caiji_sold_itemid
(item_id string comment '商品id')
comment '销量采集所需商品id'
PARTITIONED BY  (ds STRING );

insert overwrite table wl_analysis.t_wrt_caiji_sold_itemid partition(ds = '20170416')
select item_id from wl_base.t_base_ec_item_sold_dev where ds = '20170303';