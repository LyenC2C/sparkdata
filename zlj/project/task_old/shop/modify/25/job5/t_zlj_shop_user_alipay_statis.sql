-- 店铺用户认证分布

DROP TABLE IF EXISTS t_zlj_shop_user_alipay_statis;


CREATE TABLE t_zlj_shop_user_alipay_statis AS
  SELECT
    shop_id,
    count(1) AS alipay_num,
    alipay
  FROM t_zlj_shop_shop_user_level_verify
    JOIN t_base_shop_major_all

  GROUP BY shop_id, alipay;

