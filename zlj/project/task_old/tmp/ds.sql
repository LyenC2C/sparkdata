#  统计类目
SELECT
  cate_level1_name,
  root_cat_id,
  num
FROM
  (SELECT
     cate_level1_id,
     cate_level1_name
   FROM t_base_ec_dim
   GROUP BY cate_level1_id, cate_level1_name)
  t1
  JOIN
  (
    SELECT
      root_cat_id,
      count(1) AS num
    FROM
      t_zlj_userbuy_cat_count
    GROUP BY root_cat_id
  ) t2
    ON
      t1.cate_level1_id = t2.root_cat_id;


INSERT OVERWRITE TABLE t_base_ec_item_dev PARTITION(ds=%s)
SELECT
  /*+ mapjoin(t2)*/
  t1.item_id,
  t1.title,
  t1.cat_id,
  t2.cate_name        AS cat_name,
  t2.cate_level1_id   AS root_cat_id,
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
  t1.location,
  t1.ts
  (
      SELECT
      cate_id,
      cate_name,
      cate_level1_id,
      cate_level1_name
      FROM
      t_base_ec_dim
  )                      t3
JOIN
  (
    SELECT t2.*
    FROM
    (
      SELECT user_id, cast(max(ts) AS STRING)
      FROM
      user_ts GROUP BY user_id
    )t1
    JOIN item_dev t2
    ON t1.user_id =t2.user_id AND t1.ts=t2.ts
  ) t4 ON t4.cat_id = t3.cate_id
