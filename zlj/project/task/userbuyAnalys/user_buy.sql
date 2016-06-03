
--  ÓÃ»§¹ºÂò·ÖÎö




create table t_zlj_analys_userbuy_count as
select

user_id ,COUNT(1) as num
from t_base_ec_item_feed_dev group by user_id ;


select sum(num) from t_zlj_analys_userbuy_count ;



select  flag,COUNT(1) from (select  num/100 as flag from t_zlj_analys_userbuy_count )t group by flag ;




create table t_zlj_tmp as
select
t1.user_id,t1.num,buycnt
from t_zlj_analys_userbuy_count t1  join t_base_ec_tb_userinfo t2  on t1.user_id =t2.uid;

-- 7320514468      3.5887637183E10

select sum(num),sum(buycnt) from t_zlj_tmp;


SELECT flag,count(1) from (select cast(log(buycnt) as int ) as flag  from t_zlj_tmp)t group by flag;


