

use wlbase_dev;

DROP  table  if EXISTS   t_zlj_tmp_qqlink_info ;
create table t_zlj_tmp_qqlink_info as
select
t2.birthday ,
t2.gender_id ,
t2.loc ,
t2.shengxiao ,
t2.constel ,
t1.tbuid ,
t1.qq
from
 t_zlj_data_link t1
 join
 t_base_q_user_dev t2
 on (LENGTH(t2.uin)>0 and LENGTH(t1.qq)>0 and  t1.qq=t2.uin)
 ;


-- 5733005
select
count(1)
from t_zlj_tmp_qqlink_info
where
birthday like '19%'
;

create table t_zlj_tbqqlink as
select  t1.tbuid  ,t1.qq from
 t_zlj_data_link t1
 join
 t_zlj_ec_tbuid t2
 on (  t1.tbuid=t2.user_id and  t1.tbuid is not null and t2.user_id is not null)
 ;


