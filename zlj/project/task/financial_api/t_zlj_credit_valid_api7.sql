

create table t_zlj_credit_valid_api7 as

SELECT  real_name,phone,user_id_number
from t_base_credit_ppd_info
group by real_name,phone,user_id_number ;





-- real_name  姓名
-- phone      手机号
-- user_id_number  身份证