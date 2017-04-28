益芯金融信用卡代偿业务

招行信用卡->1769152985
交通信用卡->2052879865


t_base_weibo_user_fri
t_base_weibo_user_new

招商银行,交通银行 

create table wl_service.t_lel_weiboid_fansOf_176915298500002052879865
as
select fri.id
from
(select id
from
wl_base.t_base_weibo_user_fri
where
ds=20161106 and ids regexp  '1769152985|2052879865') fri
join
(select id,location from 
wl_base.t_base_weibo_user_new
where ds=20161123 and split(location,' ')[0]='上海') new 
on
fri.id = new.id;


create table wl_service.t_lel_weiboid_fansOf_zsandjtbank
as
select id
from
wl_base.t_base_weibo_user_fri
where
ds=20161106 and ids regexp  '1769152985|2052879865'




招行信用卡->1769152985
交通信用卡->2052879865
兴业信用卡-> 1892789787

create table wl_service.t_lel_weiboid_fansOf_zsjtxy_credit
as
select id
from
wl_base.t_base_weibo_user_fri
where
ds=20161106 and ids regexp  '1769152985|2052879865|1892789787'
