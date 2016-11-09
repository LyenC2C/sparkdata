create table t_wrt_tmp_weibocareer_fenbu as
select 
ltrim(rtrim(department)) as dep,
count(1) as num 
from wlbase_dev.t_base_weibo_career group by ltrim(rtrim(department))

