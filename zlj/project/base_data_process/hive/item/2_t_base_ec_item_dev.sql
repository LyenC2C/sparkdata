
use wlbase_dev;
set hive.exec.reducers.bytes.per.reducer=1000000000;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions=2000;




-- LOAD DATA  INPATH '/hive/external/wlbase_dev/t_base_ec_item_dev/ds=20150101' OVERWRITE INTO TABLE t_base_ec_item_dev PARTITION (ds='20150000') ;
-- LOAD DATA  INPATH '/data/develop/ec/tb/iteminfo_tmp/1101.dir/' OVERWRITE INTO TABLE t_base_ec_item_dev PARTITION (ds='20150001') ;

insert  OVERWRITE table t_base_ec_item_dev PARTITION(ds='20151101')

  SELECT

    /*+ mapjoin(t2)*/

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
)t2 join
    (
     select * from t_base_ec_item_dev where ds=20150001
)
t1
on t1.cat_id=t2.cate_id  ;



-- ds=from_unixtime(unix_timestamp()-86400,'yyyyMMdd'));

