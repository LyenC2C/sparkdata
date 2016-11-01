

-- 店铺用户二次购买率



create table t_zlj_shop_user as
SELECT

user_id,shop_id ,COUNT (1) as num
FROM

t_base_ec_record_dev_new

group by user_id ,shop_id ;

