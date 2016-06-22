

-- 店铺购买用户信用等级分布









-- 单独拿出一个店铺
create TABLE  t_zlj_tmp as
SELECT  *
FROM t_zlj_shop_shop_user_level_verify where shop_id =65525181 ;



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