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
        FROM
          (
            SELECT impr_c
            FROM t_zlj_feed2015_parse_v1
            WHERE item_id = 522035124540 AND LENGTH(impr_c) > 0

          ) t3
        LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word

      ) t
    GROUP BY word

  ) t1
ORDER BY num DESC
LIMIT 1000;


--加入 类目 品牌和店铺信息
CREATE TABLE t_zlj_feed2015_parse_v3_jion_cat_brand_shop
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
    FROM t_zlj_feed2015_parse_v3 t1 JOIN
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


CREATE TABLE t_zlj_user_feed_tags
  AS
    SELECT
      user_id,
      concat_ws(' ',collect_set(concat_ws('_', word, CAST(num AS STRING)))) AS feed_tags
    FROM
      (
        select user_id,word,num,ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY num DESC) AS rn
        from

          (
          SELECT
             user_id,
             word,
             count(1) AS num
           FROM
             (
               SELECT
                 user_id,
                 word
               FROM
                 (
                   SELECT
                     user_id,
                     impr_c
                   FROM
                     t_zlj_feed2015_parse_v3_jion_cat_brand_shop
                   WHERE LENGTH(impr_c) > 1
                 ) t5
               LATERAL  VIEW explode(split(impr_c, '\\|'))t1 AS word
             ) t
           GROUP BY user_id, word
          )t3
      ) t1

      where rn <20
    GROUP BY user_id



# 找个 最大的用户看看


SELECT
  item_id,
  num
FROM
  (
    SELECT
      item_id,
      count(1) AS num
    FROM t_zlj_feed2015_parse_v2
    GROUP BY item_id
  ) t
ORDER BY num DESC
LIMIT 10;


