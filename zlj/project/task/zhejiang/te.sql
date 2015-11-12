# CREATE TABLE t_zlj_base_ec_item_sale_dev
# (
#   select * FROM t_base_ec_dim
# )
#   t4 JOIN
CREATE TABLE t_zlj_base_ec_item_sale_dev
  AS
    SELECT
      /*+ mapjoin(t4)*/
      t3.*,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name
    FROM

      (SELECT
         /*+ mapjoin(t1)*/
         t2.*,
         t1.location,
         t1.city
       FROM
         (
           SELECT
             shop_id,
             location,
             regexp_replace(location, '浙江', '') AS city
           FROM t_base_ec_shop_dev
           WHERE ds = 20151107
                 AND location LIKE '%浙江%'
                 AND bc_type = 'B'
         ) t1
         JOIN
         (
           SELECT
             shop_id,
             item_id,
             s_price,
             total_sold,
             s_price * total_sold AS all_sold_price,

             ds
           FROM t_base_ec_item_sale_dev
         ) t2
           ON t1.shop_id = t2.shop_id
      ) t3
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
      ) t4 ON t3.item_id = t4.item_id;


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
