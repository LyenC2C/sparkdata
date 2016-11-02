-- 教育信息来自qq群

use wlbase_dev;

create table t_base_credit_user_qq_education AS
select qq_id,find_schools
from t_zlj_qq_find_qq_school_rst;