ds_1=$1
ds=$2

/home/hadoop/hive/bin/hive<<EOF

USE wlbase_dev;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.reducers.bytes.per.reducer = 1000000000;
SET hive.exec.dynamic.partition = TRUE;

INSERT overwrite TABLE t_zlj_base_ec_item_sale_dev_day PARTITION(ds)

SELECT
  /*+ mapjoin(t4)*/
  t5.shop_id,
  t5.item_id,
  t5.s_price,
  t5.day_sold,
  t5.day_sold_price,
  t5.location,
  cate_id,
  cate_name,
  root_cat_id,
  root_cat_name,
  brand_id,
  brand_name,
  '$ds' AS ds
FROM

  (SELECT
     /*+ mapjoin(t3)*/
     t4.*,
     t3.location
   FROM
     (
       SELECT
         shop_id,
         location
       FROM t_base_ec_shop_dev
       WHERE ds = 20151107
     ) t3
     JOIN
     (
       SELECT
         shop_id,
         t1.item_id,
         s_price,
         CASE WHEN t2.item_id IS NULL
           THEN t1.total_sold
         ELSE t1.total_sold - t2.total_sold END             AS day_sold,
         CASE WHEN t2.item_id IS NULL
           THEN s_price * t1.total_sold
         ELSE s_price * (t1.total_sold - t2.total_sold) END AS day_sold_price
       FROM

         (SELECT
            shop_id,
            item_id,
            s_price,
            total_sold
          FROM t_base_ec_item_sale_dev
          WHERE ds = '$ds'
         ) t1
         LEFT JOIN

         (SELECT
            item_id,
            total_sold
          FROM t_base_ec_item_sale_dev
          WHERE ds = '$ds_1') t2
           ON t1.item_id = t2.item_id
     ) t4
       ON t3.shop_id = t4.shop_id
  ) t5
  JOIN
  (
    SELECT
      item_id,
      cat_id,
      cate_name,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name
    FROM t_base_ec_item_dev
    WHERE ds = 20151107
  ) t6 ON t5.item_id = t6.item_id;


EOF