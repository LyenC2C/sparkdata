


-- 店铺信用等级 粉丝数 月均分布

create  table  t_zlj_shop_credit_fans_inc as
SELECT
  substring(ds,0,6) as t_month, avg(credit) as avg_credit ,avg(fans_count)  avg_fans_count ,shop_id ,shop_name
  FROM
    t_base_ec_shop_dev
group by substring(ds,0,6) ,shop_id ,shop_name

;


-- 店铺每月月销量等

select * from t_wrt_shop_monsold where shop_id in
(
'105799295',
'65525181',
'35432231',
'110999350',
'107903881',
'103889467',
'72113206',
'105777081',
'105951398',
'104738351',
'144619358',
'59288165',
'110452617',
'59559926'
)

