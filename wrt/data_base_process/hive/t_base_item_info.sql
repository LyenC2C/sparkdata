hive<<EOF
use wlbase_dev;
today=$1
LOAD DATA  INPATH '/user/wrt/temp/iteminfo_tmp' OVERWRITE INTO TABLE t_base_ec_item_dev_new PARTITION (ds=$today);

insert into table t_base_ec_item_dev_new partitions(ds=$today)
select
t1.item_id,
t1.title,
t1.cat_id,
t1.cat_name,
t1.root_cat_id ,
t1.root_cat_name,
t1.brand_id,
t1.brand_name,
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
null,
t1.ts
from
(select * from t_base_ec_item_dev where ds = 20160333)t1
left JOIN
(select * from t_base_ec_item_dev_new where ds=$today)t2
ON
t1.item_id = t2.item_id
where
t2.item_id is null;

EOF

-- select
-- case when  t2.item_id   is not null then  t2.item_id       else t1.item_id       ,
-- case when  t2.item_id   is not null then  t2.title         else t1.title         ,
-- case when  t2.item_id   is not null then  t2.cat_id        else t1.cat_id        ,
-- case when  t2.item_id   is not null then  t2.cat_name      else t1.cat_name      ,
-- case when  t2.item_id   is not null then  t2.root_cat_id   else t1.root_cat_id   ,
-- case when  t2.item_id   is not null then  t2.root_cat_name else t1.root_cat_name ,
-- case when  t2.item_id   is not null then  t2.brand_id      else t1.brand_id      ,
-- case when  t2.item_id   is not null then  t2.brand_name    else t1.brand_name    ,
-- case when  t2.item_id   is not null then  t2.bc_type       else t1.bc_type       ,
-- case when  t2.item_id   is not null then  t2.price         else t1.price         ,
-- case when  t2.item_id   is not null then  t2.price_zone    else t1.price_zone    ,
-- case when  t2.item_id   is not null then  t2.is_online     else t1.is_online     ,
-- case when  t2.item_id   is not null then  t2.off_time      else t1.off_time      ,
-- case when  t2.item_id   is not null then  t2.favor         else t1.favor         ,
-- case when  t2.item_id   is not null then  t2.seller_id     else t1.seller_id     ,
-- case when  t2.item_id   is not null then  t2.shop_id       else t1.shop_id       ,
-- case when  t2.item_id   is not null then  t2.location      else t1.location      ,
-- case when  t2.item_id   is not null then  t2.paramap       else t1.paramap       ,
-- case when  t2.item_id   is not null then  t2.sku           else ""           ,
-- case when  t2.item_id   is not null then  t2.ts            else t1.ts
-- from t_base_ec_item_dev t1  full OUTER join t_base_ec_item_dev_new t2
-- on t1.item_id =t2.item_id