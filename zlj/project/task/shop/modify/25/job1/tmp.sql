

CREATE  table t_zlj_shop_baseinfo_rn as

SELECT
  u1.main_cat_name,
  u1.shop_id,
  u1.shop_name,
  u1.desc_score_rk,
  u1.desc_score,
  u1.service_score_rk,
  u1.service_score,
  u1.wuliu_score_rk,
  u1.wuliu_score,
  u1.credit_maincat_rk,
  u1.credit,
  u1.fans_count_rk,
  u1.fans_count,
  u1.shop_mouths_rk,
  u1.shop_mouths,
  u2.credit_loc_rk,
  u2.total_locat_rn
FROM
  (
    SELECT
      y1.*,
      y2.total_maincat_rn
    FROM
      (SELECT t1.*
       FROM
         (
           SELECT
             main_cat_name,
             shop_id,
             shop_name,
             ROW_NUMBER()
             OVER (PARTITION BY main_cat_name
               ORDER BY desc_score DESC)    AS desc_score_rk,
             desc_score,
             ROW_NUMBER()
             OVER (PARTITION BY main_cat_name
               ORDER BY service_score DESC) AS service_score_rk,
             service_score,
             ROW_NUMBER()
             OVER (PARTITION BY main_cat_name
               ORDER BY wuliu_score DESC)   AS wuliu_score_rk,
             wuliu_score,
             ROW_NUMBER()
             OVER (PARTITION BY main_cat_name
               ORDER BY credit DESC)        AS credit_maincat_rk,
             credit,
             ROW_NUMBER()
             OVER (PARTITION BY main_cat_name
               ORDER BY fans_count DESC)    AS fans_count_rk,
             fans_count,
             ROW_NUMBER()
             OVER (PARTITION BY main_cat_name
               ORDER BY shopn DESC)         AS shop_mouths_rk,
             shopn                             shop_mouths
           FROM
             (SELECT
                r.*,
                cast((12 * (2017 - YEAR(STARTS)) - MONTH(STARTS)) AS FLOAT) shopn
              FROM
                t_zlj_shop_join_major r
             ) r1
         ) t1
      ) y1
      JOIN
      (SELECT
         main_cat_name,
         COUNT(1) AS total_maincat_rn
       FROM t_zlj_shop_join_major
       GROUP BY main_cat_name) y2
        ON y1.main_cat_name = y2.main_cat_name
  ) u1
  JOIN
  (
    SELECT
      y11.*,
      y12.total_rn total_locat_rn
    FROM
      (SELECT *
       FROM
         (
           SELECT
             location,
             shop_id,
             shop_name,
             ROW_NUMBER()
             OVER (PARTITION BY location
               ORDER BY credit DESC) AS credit_loc_rk
           FROM
             t_zlj_shop_join_major

         ) t11
      ) y11
      JOIN
      (SELECT
         location,
         COUNT(1) AS total_rn
       FROM t_zlj_shop_join_major
       GROUP BY location) y12
        ON y11.location = y12.location
  ) u2
    ON u1.shop_id = u2.shop_id;