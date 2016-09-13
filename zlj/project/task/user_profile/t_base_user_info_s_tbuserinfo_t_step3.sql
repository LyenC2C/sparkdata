
drop table   t_base_user_info_s_tbuserinfo_t_step3 ;

create table t_base_user_info_s_tbuserinfo_t_step3  as
select t3.*,prov as tel_prov,city as tel_city  from
(
SELECT
uid as tb_id ,
regexp_replace(alipay, 'None', '-')   as alipay,
buycnt,
verify,
regtime,
nick   as tb_nick,
location as tb_location ,
t1.tgender as qq_gender,
t1.tage as qq_age,
t1.tname as qq_name,
t1.tloc as qq_loc
from t_base_user_info_s t1  RIGHT OUTER join
(select * from t_base_ec_tb_userinfo where ds=20160608)  t2
on t1.ds=20160418 and t1.tb_id=t2.uid
)t3 LEFT join t_base_ec_loc t4 on t3.tb_id=t4.user_id ;


-- SELECT
-- COUNT(1)
-- from t_base_user_info_s t1  RIGHT OUTER join
-- (select * from t_base_ec_tb_userinfo where ds=20160608)  t2
-- on t1.ds=20160418 and t1.tb_id=t2.uid
--
-- select COUNT(1) from t_base_ec_tb_userinfo where ds=20160608