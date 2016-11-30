
--成都地区用户提取

create table wlservice.t_zlj_chengdu_weibo_user as
SELECT

*
from t_base_weibo_user where ds='20161104' and location like '%成都%' ;
