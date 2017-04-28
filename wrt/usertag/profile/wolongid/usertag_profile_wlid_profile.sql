-- 在QQ号与wlid打通上仍然会出现一对多的问题，而且是双向的
-- 首先在一个QQ号对应多个wlid上，我们暂时把不同wlid视为不同的人去进行计算，但是可以记录一下同一个QQ号的两个wlid可能是潜在的同一个人
-- 而同一个wlid对应不同的QQ号就可以暂时理解成同一个人，在处理上可以选择结合的方式。


create table wl_usertag.usertag_profile_wlid_txprofile as
select
t2.link1 as wlid,
max(t1.tx_gender) as tx_gender,
max(t1.tx_age) as tx_age,
max(t1.tx_loc) as tx_loc,
max(t1.tx_name) as tx_name
from
wl_usertag.t_wrt_profile_tx t1
JOIN
(select link1,link2 from wl_link.t_wl_links_source where dim = 'wlidqq')t2
ON
t1.qq = t2.link2
group by t2.link1;




-- 在tb号与wlid打通上仍然会出现一对多的问题，而且是双向的
-- 首先在一个tb号对应多个wlid上，暂时也可以视为不同的人但极有可能是同一个人换了手机号,这里视为不同的人首先不影响模型
-- 其次我们也无法判定哪个是他现在正在用的手机号
-- 而同一个wlid对应不同的tb号就可以暂时理解成同一个人，在处理上选择结合与保留注册时间最晚的tb账号信息的方式。

create table wl_usertag.usertag_profile_wlid_tb_rn AS
SELECT
tt2.link1 as wlid,
tt1.xy_gender,
tt1.xy_age,
tt1.xy_loc,
row_number()  OVER (PARTITION BY tt2.link1 ORDER BY tt1.regtime ) as rn
from
(
select
t1.tb_id,
regexp_replace(t2.regtime,'\\.','') as regtime,
xy_gender,
xy_age,
xy_loc
from
wl_usertag.t_wrt_profile_tb t1
left JOIN
(select tb_id,max(regtime) as regtime from wl_base.t_base_user_profile group by tb_id)t2
ON
t1.tb_id = t2.tb_id
)tt1
JOIN
(select link1,link2 from wl_link.t_wl_links_source where dim = 'wlidtb')tt2
on
tt1.tb_id = tt2.link2

create table wl_usertag.usertag_profile_wlid_tbprofile as
select
latest.wlid,
lat_xy_gender,
lat_xy_age,
lat_xy_loc,
ori_xy_gender,
ori_xy_age,
ori_xy_loc
from
(
select
wlid,
case when size(collect_list(xy_gender)) = 0 then null else collect_list(xy_gender)[0] end as lat_xy_gender,
case when size(collect_list(xy_age)) = 0 then null else collect_list(xy_age)[0] end as lat_xy_age,
case when size(collect_list(xy_loc)) = 0 then null else collect_list(xy_loc)[0] end as lat_xy_loc
from
(
SELECT
tt2.link1 as wlid,
tt1.xy_gender,
tt1.xy_age,
tt1.xy_loc,
row_number()  OVER (PARTITION BY tt2.link1 ORDER BY tt1.regtime ) as rn
from
(
select
t1.tb_id,
regexp_replace(t2.regtime,'\\.','') as regtime,
xy_gender,
xy_age,
xy_loc
from
wl_usertag.t_wrt_profile_tb t1
left JOIN
(select tb_id,max(regtime) as regtime from wl_base.t_base_user_profile group by tb_id)t2
ON
t1.tb_id = t2.tb_id
)tt1
JOIN
(select link1,link2 from wl_link.t_wl_links_source where dim = 'wlidtb')tt2
on
tt1.tb_id = tt2.link2
)t
group by wlid
)latest
JOIN
(
select
wlid,
xy_gender as ori_xy_gender,
xy_age as ori_xy_age,
xy_loc as ori_xy_loc
from
(
SELECT
tt2.link1 as wlid,
tt1.xy_gender,
tt1.xy_age,
tt1.xy_loc,
row_number()  OVER (PARTITION BY tt2.link1 ORDER BY tt1.regtime ) as rn
from
(
select
t1.tb_id,
regexp_replace(t2.regtime,'\\.','') as regtime,
xy_gender,
xy_age,
xy_loc
from
wl_usertag.t_wrt_profile_tb t1
left JOIN
(select tb_id,max(regtime) as regtime from wl_base.t_base_user_profile group by tb_id)t2
ON
t1.tb_id = t2.tb_id
)tt1
JOIN
(select link1,link2 from wl_link.t_wl_links_source where dim = 'wlidtb')tt2
on
tt1.tb_id = tt2.link2
)t
where rn = 1
group by wlid
)original
ON
latest.wlid = original.wlid;
