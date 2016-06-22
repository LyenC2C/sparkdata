


-- 店铺信用等级分布
create table t_zlj_shop_shop_user_level_verify_1 as
SELECT t2.*,alipay, verify   from
(
SELECT alipay, verify,tb_id
  FROM  t_base_user_info_s_tbuserinfo_t where tb_id is not null and alipay  is not null and length(verify)>2
)t1
join
(
select shop_id,user_id,item_id ,ds,cat_id
from  t_base_ec_record_dev_new
where shop_id is not null
)t2 on t1.tb_id=t2.user_id ;

