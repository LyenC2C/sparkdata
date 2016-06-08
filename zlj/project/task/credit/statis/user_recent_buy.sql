



create table t_zlj_credit_user_recent_buy as

SELECT
user_id,max(cast(ds as int ))  as rb_ds
from

t_base_ec_record_dev_new_simple
group by user_id ;


-- select substring(rb_ds,0,6),count(1) from t_zlj_credit_user_recent_buy
--
-- group by substring(rb_ds,0,6) ;
--
--
--
-- select count(1) from (select user_id from t_zlj_credit_user_recent_buy group by user_id) t