CREATE TABLE t_zlj_credit_user_price_statis AS
  SELECT
    user_id,
    COUNT(1)   AS times,
    sum(price) AS sum_p,
    max(price) AS max_p,
    min(price) AS min_p,
    avg(price) AS avg_p
  FROM t_base_ec_record_dev_new_simple
  GROUP BY user_id
;



SELECT  times ,count(1) from (SELECT cast(times/10 as int )  times from t_zlj_credit_user_price_statis where times>100)t group by times;

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
    THEN   'level2'
  WHEN level1 > 7
    THEN   'level3'
  WHEN level1 < 1
    THEN   'level0' END AS max_level
FROM
  (SELECT
     t.*,
     log2(max_p)  level1
   FROM t_zlj_credit_user_price_statis t
  ) y limit 10;
;

