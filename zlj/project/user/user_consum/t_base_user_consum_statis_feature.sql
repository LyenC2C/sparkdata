

create table t_base_user_consum_statis_feature as
select

t1.* ,
t2.tmall_ratio,
t2.beauty_ratio,
t2.game_ratio,
t2.edu_ratio,
t2.medical_ratio,
t2.fraud_cnt ,
t3.std_pay
from t_base_user_consum_statis_data

t1
 JOIN t_base_user_consum_statis_data_title t2  on (t1.user_id=t2.user_id )
left join t_base_user_consum_statis_user_month_stddev t3
on t1.user_id =t2.user_id
;
