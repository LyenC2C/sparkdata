create table wlbase_dev.t_base_ec_record_dev_new_telindex as
select
t1.tel_index as tel_index,
t1.rn as rn,
t2.item_id  as item_id,
t2.feed_id  as feed_id,
t2.user_id  as user_id,
t2.content_length as content_length,
t2.annoy as annoy,
t2.dsn as dsn,
t2.datediff as  datediff,
t2.sku as  sku,
t2.title as  title,
t2.cat_id as  cat_id,
t2.root_cat_id as  root_cat_id,
t2.root_cat_name as  root_cat_name,
t2.brand_id as  brand_id,
t2.brand_name as  brand_name,
t2.bc_type as  bc_type,
t2.price as price,
t2.shop_id as  shop_id,
t2.location as  location
from
t_zlj_phone_rank_index t1
join
t_base_ec_record_dev_new t2
on
t1.id = t2.user_id
and
t2.ds = 'true'