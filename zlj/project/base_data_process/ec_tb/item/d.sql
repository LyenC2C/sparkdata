USE wlbase_dev;
SET hive.exec.reducers.bytes.per.reducer = 1000000000;
SET hive.exec.dynamic.partition = TRUE;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET hive.exec.max.dynamic.partitions = 2000;


LOAD DATA  INPATH '/hive/external/wlbase_dev/t_base_ec_item_dev/ds=20150101' OVERWRITE INTO TABLE t_base_ec_item_dev PARTITION (ds='20150000');


INSERT OVERWRITE TABLE t_base_ec_item_dev PARTITION(ds)


(SELECT

/*+ mapjoin(t2)*/

t1.item_id,
t1.title,
t1.cat_id,
t2.cate_name AS cat_name,
t2.cate_level1_id AS root_cat_id,
t2.cate_level1_name AS root_cat_name,
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
,
cast(from_unixtime(unix_timestamp()-86400, 'yyyyMMdd') AS STRING) ds
FROM
(

SELECT
cate_id,
cate_name,
cate_level1_id,
cate_level1_name
FROM
t_base_ec_dim
WHERE ds=20151023
)t2 JOIN
(

 SELECT
 (
 SELECT  * from
   t_base_ec_item_dev
    WHERE ds= 20151029 OR ds = 20150000
  )t1

  FULL JOIN
  (SELECT * FROM t_base_ec_item_dev WHERE ds='pt') t2
  on t1.item_id =t2.item_id

ON t1.item_id=t2.item_id;

)
t1
ON t1.cat_id=t2.cate_id
)t3;


-- ds=from_unixtime(unix_timestamp()-86400,'yyyyMMdd'));

