# CREATE TABLE t_zlj_base_ec_item_sale_dev
# (
#   select * FROM t_base_ec_dim
# )
#   t4 JOIN
insert overwrite table t_zlj_base_ec_item_sale_dev_day partition(ds)

    SELECT
      /*+ mapjoin(t4)*/
      t5.*,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name
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
           #            SELECT
           #              shop_id,
           #              item_id,
           #              s_price,
           #              total_sold,
           #              s_price * total_sold AS all_sold_price,
           #
           #              ds
           #            FROM t_base_ec_item_sale_dev
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
          root_cat_id,
          root_cat_name,
          brand_id,
          brand_name
        FROM t_base_ec_item_dev
        WHERE ds = 20151107
      ) t6 ON t5.item_id = t6.item_id;


SELECT *
FROM

  t_zlj_base_ec_item_sale_dev
GROUP BY city;


--浙江 总销量


SELECT
  /*+ mapjoin(t1)*/
  sum(all_sold_price),
  sum(total_sold)
FROM
  (
    SELECT shop_id,
    FROM t_base_ec_shop_dev
    WHERE ds = 20151107
          AND location LIKE '%浙江%'
          AND bc_type = 'B') t1
  JOIN
  (SELECT
     shop_id,
     s_price * total_sold AS all_sold_price,
     total_sold,
     ds
   FROM
     t_base_ec_item_sale_dev
   WHERE bc_type = 'b'
  ) t2 ON t1.shop_id = t2.shop_id
