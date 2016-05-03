
--  ÓÃ»§¹ºÂò·ÖÎö




create table t_zlj_analys_userbuy_count as
select

user_id ,COUNT(1) as num
from t_base_ec_item_feed_dev group by user_id ;


select sum(num) from t_zlj_analys_userbuy_count ;



select  flag,COUNT(1) from (select  num/100 as flag from t_zlj_analys_userbuy_count )t group by flag ;


7994250828


35996028295
