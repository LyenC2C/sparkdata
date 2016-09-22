
-- 用户消费统计特征表

drop table t_base_user_consum_statis_user_month_stddev;
create table t_base_user_consum_statis_user_month_stddev  as
select
user_id,
stddev(price) as std_pay
from
(
select user_id,substr(dsn,1,6) as mth,sum(price) as price from t_base_ec_record_dev_new
where ds ='true' and dsn>20160101
group by user_id, substr(dsn,1,6)
) a
group by user_id;


