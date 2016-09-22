

create table t_base_user_consum_statis_feature as
select

t1.* ,t2.std_pay
from t_base_user_consum_statis_data

t1 left join t_base_user_consum_statis_user_month_stddev t2

on t1.user_id =t2.user_id
;
