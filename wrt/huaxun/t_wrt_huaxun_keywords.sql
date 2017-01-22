drop table wlservice.t_wrt_huaxun_keywords_tmp;
create table wlservice.t_wrt_huaxun_keywords_tmp as
select user_id,
count(1) as buy_count,
max(case when title rlike '炒股' then '炒股' else "" end) as k1,
max(case when title rlike '股票' then '股票' else "" end) as k2,
max(case when title rlike '操盘' then '操盘' else "" end) as k3
from
wl_base.t_base_ec_record_dev_new where ds = 'true' and price > 200 and title rlike '炒股|股票|操盘'
group by user_id
;

drop table wlservice.t_wrt_huaxun_tb_id_keywords;
create table wlservice.t_wrt_huaxun_tb_id_keywords as
select
user_id,
buy_count,
concat(k1,k2,k3) as keywords
from wlservice.t_wrt_huaxun_keywords_tmp;


drop table wlservice.t_wrt_huaxun_tb_id_keywords_city;
create table wlservice.t_wrt_huaxun_tb_id_keywords_city AS
select
t1.*,
case
when t2.tel_loc rlike '北京|上海|天津' then split(t2.tel_loc,' ')[0]
when t2.tel_loc rlike '广州|深圳|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京' then split(t2.tel_loc,' ')[1]
END as city
from
wlservice.t_wrt_huaxun_tb_id_keywords t1
JOIN
(select * from wlbase_dev.t_base_user_profile where tel_loc rlike
'北京|上海|广州|深圳|天津|杭州|苏州|宁波|温州|绍兴|江阴|萧山|东莞|佛山|无锡|常州|南京')t2
ON
t1.user_id = t2.tb_id;
