drop table wlservice.t_wrt_huaxun_weiboid_keywords;
create table wlservice.t_wrt_huaxun_weiboid_keywords (
weiboid string,
keywords string,
num string
);


drop table wlservice.t_wrt_huaxun_weiboid_keywords_city;
create table wlservice.t_wrt_huaxun_weiboid_keywords_city as
select t1.weiboid,
t1.num,
t1.keywords,
case
when t2.location rlike '北京|上海|天津' then split(t2.location,' ')[0]
when t2.location rlike '广州|深圳|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京' then split(t2.location,' ')[1]
END
as city
from
(select * from wlservice.t_wrt_huaxun_weiboid_keywords )t1
join
(select idstr,location from wlbase_dev.t_base_weibo_user_new where location rlike
'北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京' )t2
ON
t1.weiboid = t2.idstr;
