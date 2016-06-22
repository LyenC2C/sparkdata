

-- 店铺用户认证分布
CREATE  TABLE  t_zlj_shop_user_alipay_statis as
SELECT  shop_id, count(1) as alipay_num ,alipay
  FROM  t_zlj_shop_shop_user_level_verify
JOIN t_base_shop_major_all

group by shop_id ,alipay
;

