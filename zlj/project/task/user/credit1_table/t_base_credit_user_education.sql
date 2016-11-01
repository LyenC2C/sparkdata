-- 教育信息来自qq微博

create table t_base_credit_user_education AS
select id as qq_weibo_id, year1, background1, school1, department1, year2, background2, school2, department2,
year3, background3,school3, department3
from
t_qqweibo_user_info where year1 <> '-' ;
