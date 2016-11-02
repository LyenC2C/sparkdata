



-- 店铺用户的等级分布

CREATE TABLE t_zlj_shop_user_verify_statis AS

  SELECT
    shop_id,
    count(1) AS verify_num,
    verify
  FROM t_zlj_shop_shop_user_level_verify

  GROUP BY shop_id, verify;
