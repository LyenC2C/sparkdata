



SELECT  *  FROM    t_base_ec_shop_dev where shop_id in ('65525181') ;









SELECT  shop_id, count(1),verify
  FROM  t_zlj_shop_shop_user_level_verify
  where shop_id in
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
  group by shop_id ,verify
;




SELECT  shop_id, count(1),alipay
  FROM  t_zlj_shop_shop_user_level_verify


JOIN t_base_shop_major_all


  where shop_id in
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
group by shop_id ,alipay
;





create TABLE  t_zlj_tmp as
SELECT  *
FROM t_zlj_shop_shop_user_level_verify_1 where shop_id =65525181

SELECT
sum( case when  ds<20160101 then 1 else 0 end ) as d1,
sum( case when  ds<20160201 then 1 else 0 end) as d2,
sum( case when  ds<20160301 then 1 else 0 end ) as d3,
sum( case when  ds<20160401 then 1 else 0 end) as d4,
sum( case when  ds<20160501 then 1 else 0 end) as d5,
sum( case when  ds<20160601 then 1 else 0 end ) as d6,
sum( case when  ds<20160701 then 1 else 0 end ) as d7
FROM t_zlj_tmp ;




-- 用户增长
-- 22088   24698   25957   28812   31811   33655   33939
SELECT
COUNT (DISTINCT (case when  ds<20160101 then user_id else NULL end)) as d1 ,
COUNT (DISTINCT (case when  ds<20160201 then user_id else NULL end)) as d1 ,
COUNT (DISTINCT (case when  ds<20160301 then user_id else NULL end)) as d1 ,
COUNT (DISTINCT (case when  ds<20160401 then user_id else NULL end)) as d1 ,
COUNT (DISTINCT (case when  ds<20160501 then user_id else NULL end)) as d1 ,
COUNT (DISTINCT (case when  ds<20160601 then user_id else NULL end)) as d1 ,
COUNT (DISTINCT (case when  ds<20160701 then user_id else NULL end)) as d1
FROM t_zlj_tmp ;