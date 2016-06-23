--  店铺用户月增长
-- job 跑的很慢,所以慎重。 一个月跑一次
-- 下个月增加一个月


CREATE TABLE IF NOT EXISTS t_zlj_shop_user_inc AS
  SELECT
    shop_id,
    COUNT(DISTINCT (CASE WHEN ds < 20160101
      THEN user_id
                    ELSE NULL END)) AS month1,
    COUNT(DISTINCT (CASE WHEN ds < 20160201
      THEN user_id
                    ELSE NULL END)) AS month2,
    COUNT(DISTINCT (CASE WHEN ds < 20160301
      THEN user_id
                    ELSE NULL END)) AS month3,
    COUNT(DISTINCT (CASE WHEN ds < 20160401
      THEN user_id
                    ELSE NULL END)) AS month4,
    COUNT(DISTINCT (CASE WHEN ds < 20160501
      THEN user_id
                    ELSE NULL END)) AS month5,
    COUNT(DISTINCT (CASE WHEN ds < 20160601
      THEN user_id
                    ELSE NULL END)) AS month6,
    COUNT(DISTINCT (CASE WHEN ds < 20160701
      THEN user_id
                    ELSE NULL END)) AS month7


  FROM t_zlj_shop_shop_user_level_verify
  GROUP BY shop_id;