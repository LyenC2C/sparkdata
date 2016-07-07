-- 来自qq微博的工作信息

create table t_base_credit_user_education AS
select id as qq_weibo_id, com_startyear1, com_endyear1, com_comname1, com_depname1, com_startyear2, com_endyear2,
com_comname2, com_depname2, com_startyear3, com_endyear3, com_comname3, com_depname3
from
t_qqweibo_user_info where com_startyear1 <> '-' ;