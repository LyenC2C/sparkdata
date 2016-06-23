


DROP TABLE  IF EXISTS  t_zlj_shop_user_age;


CREATE TABLE t_zlj_shop_user_age AS
  SELECT
    shop_id,

    sum(CASE WHEN tage < 10 AND tage > 69
      THEN 1
        ELSE 0 END) level0,
    sum(CASE WHEN tage >= 10 AND tage <= 24
      THEN 1
        ELSE 0 END) level1,
    sum(CASE WHEN tage >= 25 AND tage <= 29
      THEN 1
        ELSE 0 END) level2,
    sum(CASE WHEN tage >= 30 AND tage <= 34
      THEN 1
        ELSE 0 END) level3,
    sum(CASE WHEN tage >= 35 AND tage <= 39
      THEN 1
        ELSE 0 END) level4,
    sum(CASE WHEN tage >= 40 AND tage <= 49
      THEN 1
        ELSE 0 END) level5,
    sum(CASE WHEN tage >= 50 AND tage <= 69
      THEN 1
        ELSE 0 END) level6
  FROM t_zlj_shop_shop_user_level_verify_1
  WHERE tage > 10 AND tage < 80
  GROUP BY shop_id;

-- SELECT  * FROM  t_zlj_shop_user_inc where shop_id=65525181

-- 65525181
--
-- 10-24  level1 24698
-- 25-29  level2 25957
-- 30-34  level3 28812
-- 35-39  level4 31811
-- 40-49  level5 33655
-- 50-69  level6 33939