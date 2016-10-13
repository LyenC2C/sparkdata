
drop table   t_base_user_info_s_tbuserinfo_t_step3 ;

select h_provincename,h_city from
t_base_qq_weibo_user_info t1
join t1

-- SELECT
-- COUNT(1)
-- from t_base_user_info_s t1  RIGHT OUTER join
-- (select * from t_base_ec_tb_userinfo where ds=20160608)  t2
-- on t1.ds=20160418 and t1.tb_id=t2.uid
--
-- select COUNT(1) from t_base_ec_tb_userinfo where ds=20160608


select l_provincename ,l_cityname  ,b_year from
t_base_qq_weibo_user_info  limit 10;



select
t2.uid  as tb_id , CAST(t3.gender-1 as string) as tgender, (2016-b_year )  as tage , nickname  as tname ,concat_ws('\t',l_provincename , l_city) as tloc
from t_base_qq_weibo_user_info t3

join t_base_uid t2   where t2.ds='tb-qqwb'  and t3.id=t2.id1