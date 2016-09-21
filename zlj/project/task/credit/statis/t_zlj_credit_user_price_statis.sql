 DROP TABLE t_zlj_credit_user_price_statis;


--  基本统计信息
CREATE TABLE t_zlj_credit_user_price_statis_back AS
  SELECT
    user_id,
    COUNT(1)   AS times,
    sum(price) AS sum_p,
    max(price) AS max_p,
    min(price) AS min_p,
    avg(price) AS avg_p ,
    sum(case when bc_type='B' then price else '0' end)/sum(price) as tmallbuy_ratio,
stddev(price) as price_std

  FROM t_base_ec_record_dev_new  where ds='true'
  GROUP BY user_id  HAVING count(1)<7000
 ;






--  类目分析
CREATE TABLE t_zlj_credit_user_price_statis_dim AS

sum( case when root_cat_name in ('彩妆/香水/美妆工具','个人护理/保健/按摩器材','洗护清洁剂/卫生巾/纸/香薰','俪人购(俪人购专用)') then price else '0' end)/sum(price) as beauty_consum_ratio,
sum( case when root_cat_name in ('彩妆/香水/美妆工具','个人护理/保健/按摩器材','洗护清洁剂/卫生巾/纸/香薰','俪人购(俪人购专用)') then price else '0' end) as beauty_consum,
  FROM t_base_ec_record_dev_new  where ds='true'
  GROUP BY user_id  HAVING count(1)<7000

   ;



SELECT  count(1) from t_zlj_credit_user_price_statis where times>10000;

SELECT
  times,
  count(1)
FROM (SELECT cast(times / 10 AS INT) times
      FROM t_zlj_credit_user_price_statis
      WHERE times > 100) t
GROUP BY times;

SELECT
  level1,
  COUNT(1)
FROM (SELECT CAST(log2(max_p) AS INT) level1
      FROM t_zlj_credit_user_price_statis) t
GROUP BY level1;


SELECT
  level1,
  COUNT(1)
FROM (SELECT CAST(log2(min_p) AS INT) level1
      FROM t_zlj_credit_user_price_statis) t
GROUP BY level1;


SELECT
  user_id,
  times,
  sum_p,
  max_p,
  min_p,
  avg_p,
  CASE WHEN level1 > 0 AND level1 < 5
    THEN 'level1'
  WHEN level1 > 4 AND level1 < 8
    THEN 'level2'
  WHEN level1 > 7
    THEN 'level3'
  WHEN level1 < 1
    THEN 'level0' END AS max_level
FROM
  (SELECT
     t.*,
     log2(max_p) level1
   FROM t_zlj_credit_user_price_statis t
  ) y
LIMIT 10;
;

SELECT
  user_id,
  level_,
  level_time
FROM
  (
    SELECT
      user_id,
      level_,
      level_time,
      ROW_NUMBER()
      OVER (PARTITION BY user_id
        ORDER BY level_time DESC) rn
    FROM
      (
        SELECT
          user_id,
          level_,
          count(1) level_time
        FROM
          (
            SELECT
              user_id,
              CASE WHEN log2(price) <= 2.5
                THEN 'level_1'
              WHEN log2(price) > 2.5 AND log2(price) <= 3.5
                THEN 'level_2'
              WHEN log2(price) > 3.5 AND log2(price) <= 6
                THEN 'level_3'
              WHEN log2(price) > 6 AND log2(price) <= 7.5
                THEN 'level_4'
              WHEN log2(price) > 7.5
                THEN 'level_5' END AS level_
            FROM t_base_ec_record_dev_new_simple
            WHERE price IS NOT NULL
          ) t
        GROUP BY user_id, level_
      ) t2

  ) t3
WHERE rn = 1;