create table t_wrt_medicine like wlbase_dev.t_base_ec_item_dev_new;



insert overwrite table t_wrt_medicine PARTITION (ds='shop_yaofang')
select
t2.item_id,
t2.title,
t2.cat_id,
t2.cat_name,
t2.root_cat_id,
t2.root_cat_name,
t2.brand_id,
t2.brand_name,
t2.bc_type,
t2.price,
t2.price_zone,
t2.is_online,
t2.off_time,
t2.favor,
t2.seller_id,
t2.shop_id,
t2.location,
t2.paramap,
t2.sku,
t2.ts
from
(select shop_id from wlbase_dev.t_base_ec_shop_dev where shop_name like "%药房%" and ds = 20160331)t1
JOIN
(select * from wlbase_dev.t_base_ec_item_dev_new where ds = 20160513)t2
ON
t1.shop_id = t2.shop_id


insert overwrite table t_wrt_medicine PARTITION (ds='cat_3_id')
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
from wlbase_dev.t_base_ec_item_dev_new where ds = 20160513 and (root_cat_id = 122966004 or root_cat_id = 50023717 or root_cat_id = 123690003);


create table t_wrt_tmp_medicine as
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
from t_wrt_medicine
group by
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

