SELECT

  word,
  num
FROM
  (
    SELECT
     word,
     count(1) AS num
   FROM
     (
       SELECT word
       from
              (
                  SELECT impr_c
                  FROM t_zlj_feed2015_parse_v1 WHERE item_id=522035124540 AND LENGTH(impr_c)>0

              ) t3
       LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word

     ) t
   group by word

  ) t1
ORDER BY num  DESC LIMIT 1000 ;


--加入 类目 品牌和店铺信息
CREATE TABLE t_zlj_feed2015_parse_v1_jion_cat_brand_shop
  AS
    SELECT
      t1.*,
      cat_id,
      cat_name,
      root_cat_id,
      root_cat_name,
      brand_id,
      brand_name,
      shop_id
    FROM t_zlj_feed2015_parse_v1 t1 JOIN
      (SELECT
         item_id,
         cat_id,
         cat_name,
         root_cat_id,
         root_cat_name,
         brand_id,
         brand_name,
         shop_id
       FROM t_base_ec_item_dev
       WHERE ds = 20160105) t2

        ON t1.item_id = t2.item_id;


SELECT

  word,
  num
FROM
  (
    SELECT
     word,
     count(1) AS num
   FROM
     (
       SELECT word
       from
             t_zlj_temp
       LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word

     ) t
   group by word

  ) t1
ORDER BY num  DESC LIMIT 1000 ;