insert into table t_wrt_znk_iteminfo partition(ds = 20160826)
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
(
select * from wlbase_dev.t_base_ec_item_dev_new where ds = 20160825
and (cat_id = 121954006 or cat_id = 122326002 or cat_id = 50012481)
)t1
FULL JOIN
(
select * from t_wrt_znk_iteminfo where ds = 20160825 --采集的itemid
)t2
ON
t1.item_id = t2.item_id;

insert into table t_wrt_znk_iteminfo partition(ds = 20160829)
select
item_id,
title,
cat_id,
cat_name,
root_cat_id,
root_cat_name,
brand_id,
brand_name,
bc_type,
price,
price_zone,
is_online,
off_time,
favor,
seller_id,
shop_id,
location,
paramap,
sku,
ts
from t_wrt_znk_iteminfo where ds = 20160826 and paramap is not null;


select brand_id,brand_name,count(1) as brand_count,max(item_id) as item_id from t_wrt_znk_iteminfo where ds = 20160829 group by brand_id,brand_name)t1

-- 0830的分区用于存放新的品牌名称
insert into table t_wrt_znk_iteminfo partition(ds = 20160830)
select
t1.item_id,
t1.title,
t1.cat_id,
t1.cat_name,
t1.root_cat_id,
t1.root_cat_name,
t1.brand_id,
t2.brand_name,
t1.bc_type,
t1.price,
t1.price_zone,
t1.is_online,
t1.off_time,
t1.favor,
t1.seller_id,
t1.shop_id,
t1.location,
t1.paramap,
t1.sku,
t1.ts
from
(select * from t_wrt_znk_iteminfo where ds = 20160829)t1
left JOIN
(select * from t_wrt_znk_brandid_name)t2
ON
t1.brand_id = t2.brand_id;


-- 统计品牌购买次数,并降序排序
select
t1.brand_name,sum(t2.buy_count)
FROM
(select item_id,brand_name from t_wrt_znk_iteminfo where ds = 20160830)t1
JOIN
(select item_id,count(1) as buy_count from t_wrt_znk_record where ds = 20160829 group by item_id)t2
ON
t1.item_id = t2.item_id
group by
t1.brand_name

--筛选出和record表都有的商品，拿出来并用脚本把不能解析的
create table t_wrt_tmp_znk_item_iteminfo as
select
t1.item_id,
t1.title,
t1.brand_name,
t1.paramap["尺码"],
t1.paramap["成人纸尿裤护理品"],
t1.paramap["包装数量(片)"],
t1.price
FROM
(select * from t_wrt_znk_iteminfo where ds = 20160830)t1
JOIN
(select item_id from t_wrt_znk_record where ds = 20160829 group by item_id)t2
ON
t1.item_id = t2.item_id

