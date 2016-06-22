
-- 店铺用户购买记录 join 用户信用等级
create table t_zlj_shop_shop_user_level_verify_1  as
SELECT t2.*,t1.*  from
(
SELECT *
  FROM  t_base_user_info_s_tbuserinfo_t where tb_id is not null
)t1
join
(
select shop_id,user_id,item_id ,ds,cat_id
from  t_base_ec_record_dev_new
where shop_id is not null
)t2 on t1.tb_id=t2.user_id ;