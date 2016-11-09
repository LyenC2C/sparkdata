create table t_wrt_repair_iteminfo like wlbase_dev.t_base_ec_item_dev_new;
LOAD DATA  INPATH '/user/wrt/temp/repair_iteminfo_tmp' OVERWRITE INTO TABLE t_wrt_repair_iteminfo PARTITION (ds='20160814');
LOAD DATA  INPATH '/user/wrt/temp/repair_iteminfo_tmp' OVERWRITE INTO TABLE t_wrt_repair_iteminfo PARTITION (ds='20160823');

insert into table wlbase_dev.t_base_ec_item_dev_new partition(ds = '20160824')
select
case when t2.item_id is null then t1.item_id else t2.item_id end,
case when t2.title is null then t1.title else t2.title end,
case when t2.cat_id is null then t1.cat_id else t2.cat_id end,
case when t2.cat_name is null then t1.cat_name else t2.cat_name end,
case when t2.root_cat_id is null then t1.root_cat_id else t2.root_cat_id end,
case when t2.root_cat_name is null then t1.root_cat_name else t2.root_cat_name end,
case when t2.brand_id is null then t1.brand_id else t2.brand_id end,
case when t2.brand_name is null then t1.brand_name else t2.brand_name end,
case when t2.bc_type is null then t1.bc_type else t2.bc_type end,
case when t2.price is null then t1.price else t2.price end,
case when t2.price_zone is null then t1.price_zone else t2.price_zone end,
case when t2.is_online is null then t1.is_online else t2.is_online end,
case when t2.off_time is null then t1.off_time else t2.off_time end,
case when t2.favor is null then t1.favor else t2.favor end,
case when t2.seller_id is null then t1.seller_id else t2.seller_id end,
case when t2.shop_id is null then t1.shop_id else t2.shop_id end,
case when t2.location is null then t1.location else t2.location end,
case when t2.paramap is null then t1.paramap else t2.paramap end,
case when t2.sku is null then t1.sku else t2.sku end,
case when t2.ts is null then t1.ts else t2.ts end
from
(select * from wlbase_dev.t_base_ec_item_dev_new where ds = 20160814)t1
full join
(select * from t_wrt_repair_iteminfo where ds = 20160814)t2
on
t2.item_id = t1.item_id;

insert into table wlbase_dev.t_base_ec_item_dev_new partition(ds = '20160825')
select
case when t2.item_id is null then t1.item_id else t2.item_id end,
case when t2.title is null then t1.title else t2.title end,
case when t2.cat_id is null then t1.cat_id else t2.cat_id end,
case when t2.cat_name is null then t1.cat_name else t2.cat_name end,
case when t2.root_cat_id is null then t1.root_cat_id else t2.root_cat_id end,
case when t2.root_cat_name is null then t1.root_cat_name else t2.root_cat_name end,
case when t2.brand_id is null then t1.brand_id else t2.brand_id end,
case when t2.brand_name is null then t1.brand_name else t2.brand_name end,
case when t2.bc_type is null then t1.bc_type else t2.bc_type end,
case when t2.price is null then t1.price else t2.price end,
case when t2.price_zone is null then t1.price_zone else t2.price_zone end,
case when t2.is_online is null then t1.is_online else t2.is_online end,
case when t2.off_time is null then t1.off_time else t2.off_time end,
case when t2.favor is null then t1.favor else t2.favor end,
case when t2.seller_id is null then t1.seller_id else t2.seller_id end,
case when t2.shop_id is null then t1.shop_id else t2.shop_id end,
case when t2.location is null then t1.location else t2.location end,
case when t2.paramap is null then t1.paramap else t2.paramap end,
case when t2.sku is null then t1.sku else t2.sku end,
case when t2.ts is null then t1.ts else t2.ts end
from
(select * from wlbase_dev.t_base_ec_item_dev_new where ds = 20160824)t1
full join
(select * from t_wrt_repair_iteminfo where ds = 20160823)t2
on
t2.item_id = t1.item_id;