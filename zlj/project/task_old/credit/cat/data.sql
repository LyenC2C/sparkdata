-- 用户类目数量

create table t_zlj_credit_user_catnum as

select user_id ,count(1) as num
from
(
select user_id ,root_cat_id
from t_base_ec_record_dev_new_simple GROUP  by user_id ,root_cat_id
)t group by user_id ;