

item_ds=$1


/home/wrt/hive/bin/hive<<EOF


use wlbase_dev;

LOAD DATA  INPATH '/user/zlj/data/temp/t_base_ec_item_dev_tmp' OVERWRITE INTO TABLE t_base_ec_item_dev_zlj_test PARTITION (ds='20150000');

insert  OVERWRITE table t_base_ec_item_dev PARTITION(ds='$item_ds')
  SELECT  /*+ mapjoin(t2)*/
t1.item_id,
t1.title,
t1.cat_id,
t2.cate_name as cat_name,
t2.cate_level1_id as root_cat_id,
t2.cate_level1_name as root_cat_name,
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
t1.ts
  from
(
SELECT
cate_id,
cate_name,
cate_level1_id,
cate_level1_name
FROM
t_base_ec_dim
where  ds=20151023
)t2 join  t_base_ec_item_dev_zlj_test  t1 on t1.cat_id=t2.cate_id;


  EOF