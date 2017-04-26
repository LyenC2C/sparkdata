create EXTERNAL table wl_analysis.t_wrt_caiji_shopid_shopitemc (
shop_id string comment 'shopid'
)
comment '采集shopitem_c所需shopid'
PARTITIONED BY  (ds STRING )
location '/commit/ids_4_crawler/t_caiji_shopid_shopitemc';

insert overwrite table wl_analysis.t_wrt_caiji_shopid_shopitemc partition(ds = '20170425')
select shop_id from (
select shop_id,item_count from wl_base.t_base_ec_shop_dev_new where ds = '20170415'and bc_type = 'C'
order by item_count
)t;


