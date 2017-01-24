
-- 简单获取有效用户的关系数据
create table t_base_weibo_user_fri_effect_user as
select
t2.id,t2.ids
from
(
select
id
from   t_base_weibo_user_new where ds='20161213'
  )t1

  join
t_base_weibo_user_fri t2  on t1.id =t2.id where t2.ds='20161106' ;