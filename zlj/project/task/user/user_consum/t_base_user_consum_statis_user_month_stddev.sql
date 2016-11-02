
-- 用户消费统计特征表

drop table t_base_user_consum_statis_user_month_stddev;
create table t_base_user_consum_statis_user_month_stddev  as
select
user_id,
stddev(sum_price) as std_pay ,

from
(
select user_id,substr(dsn,1,6) as mth,sum(price) as sum_price ,
max(price) as max_price,
min(price) as min_price ,
max(price)-min(price) as  price_span
from t_base_ec_record_dev_new_simple
where ds ='true' and dsn>20160101
group by user_id, substr(dsn,1,6)
) a
group by user_id
;


-- 月均次数



SELECT count(1)
from wlservice.t_tc_xiaomihu_user1 t1 join t_base_uid_tmp t2 where t2.ds='ttinfo' and t1.user_id =t2.id1;