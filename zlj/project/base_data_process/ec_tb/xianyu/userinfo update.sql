

drop table t_base_user_info_s_tbuserinfo_t_0728 ;

create table t_base_user_info_s_tbuserinfo_t_0728  as

select
tb_id ,
case when gender in (0,1) then gender else tgender  end as tgender ,
case when birthday like '%19%' then  2016 -YEAR(birthday )  else tage  end as tage ,

tname ,
tloc,
alipay,
buycnt,
verify,
regtime,
t4.nick,

case when  LENGTH(city)>1 then city else tel_city end as tel_city ,
case when  LENGTH(province)>1 then province else tel_prov end as tel_prov

from
(
SELECT
t1.* ,t2.province

from t_base_ec_tb_xianyu_userinfo t1 join
t_base_city_province_dev t2

on t1.city=t2.city

)t3
  join t_base_user_info_s_tbuserinfo_t t4

on t3.userid =t4.tb_id ;



SELECT
t1.* ,t2.province

from t_base_ec_tb_xianyu_userinfo t1 join
t_base_city_province_dev t2

on t1.city=t2.city limit 10;