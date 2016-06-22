


-- 店铺用户的等级分布

create table t_zlj_shop_user_verify_statis as

SELECT  shop_id, count(1) as verify_num,verify
  FROM  t_zlj_shop_shop_user_level_verify

  group by shop_id ,verify
;
