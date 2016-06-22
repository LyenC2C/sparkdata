


-- 店铺信用等级 粉丝数 月均分布

create  table  t_zlj_shop_credit_fans_inc as
SELECT
  substring(ds,0,6) as t_month, avg(credit) as avg_credit ,avg(fans_count)  avg_fans_count ,shop_id ,shop_name
  FROM
    t_base_ec_shop_dev
group by substring(ds,0,6) ,shop_id ,shop_name

;


