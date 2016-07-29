

-- 更新用户信息
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
tel_city ,
tel_prov

from
t_base_ec_tb_xianyu_userinfo t3
join t_base_user_info_s_tbuserinfo_t t4
on t3.userid = t4.tb_id
;



create table t_base_ec_tb_xianyu_item_0728_dim  AS

SELECT t2.* ,t1.cate_id,cate_name , cate_level1_id ,cate_level1_name
from t_base_ec_dim  t1 join t_base_ec_tb_xianyu_item t2
on t2.categoryid=t1.cate_id
;