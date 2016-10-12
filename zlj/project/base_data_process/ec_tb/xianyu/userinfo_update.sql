
-- 核心数据
-- 更新用户信息
-- 优先使用腾讯信息
drop table t_base_user_info_s_tbuserinfo_t ;

create table t_base_user_info_s_tbuserinfo_t as

SELECT
tb_id ,
case when tgender in ('0','1') then tgender else CAST (t2.gender+10 as string)  end as tgender ,
 tage ,
tname ,
tloc,
alipay,
buycnt,
verify,
regtime,
nick,
tel_city ,
tel_prov

FROM
(
select
tb_id ,
case when tgender in ('0','1') then tgender else gender  end as tgender ,
case when birthday like '%19%' then  2016-YEAR(birthday )  else tage  end as tage ,
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
RIGHT  join t_base_user_info_s_tbuserinfo_join t4
on t3.userid = t4.tb_id

)t1 left join t_zlj_model_user_gender t2 on t1.tb_id=t2.id
;



-- create table t_base_ec_tb_xianyu_item_0728_dim  AS
--
-- SELECT t2.* ,t1.cate_id,cate_name , cate_level1_id ,cate_level1_name
-- from t_base_ec_dim  t1 join t_base_ec_tb_xianyu_item t2
-- on t2.categoryid=t1.cate_id
-- ;