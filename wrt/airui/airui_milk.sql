
drop table wl_service.t_wrt_airui_milk_userid;
create table wl_service.t_wrt_airui_milk_userid as
select t1.user_id from
(
select
user_id
from wl_analysis.t_base_record_cate_simple_ds
where
cat_id in ('50016427','50012392','50012393','50012391','50009865')
and
ds >= '201701' and ds <= '201703'
group by user_id
)t1
JOIN
(
select tb_id from
wl_base.t_base_user_profile_telindex
where
((qq_age > 10 and qq_age < 60) or (xianyu_birthday > 10 and xianyu_birthday < 60))
and
(qq_gender <> '-' or xianyu_gender <> '-')
and
(tel_loc <> '-')
)t2
ON
t1.user_id = t2.tb_id
JOIN
(
select
user_id,count(1) as ct
from
wl_analysis.t_base_record_cate_simple_ds
where ds > '20150000' and ds < '20170400'
group by user_id
having ct > 10 and ct < 1000
)t3
ON
t1.user_id = t3.user_id
limit 1500;


drop table wl_service.t_wrt_airui_kouhong_record;
create table wl_service.t_wrt_airui_kouhong_record as
select
cast(t2.user_id as bigint) * 777 + 632823009 as user_id,
t2.item_id,
regexp_replace(concat('https://item.taobao.com/item.htm?id=', cast (t2.item_id as string))," ","") as url,
regexp_replace(t2.title," ","") as title,
round(t2.price,2) as price,
t2.buy_date,
regexp_replace(t2.brand_name," ","") as brand_name,
regexp_replace(t1.cate_name," ","") as cate_name,
regexp_replace(t2.root_cat_name," ","") as root_cat_name,
regexp_replace(t2.`location`," ","") as loc,
t2.item_type
from
wl_base.t_base_ec_dim t1
join
(SELECT user_id,item_id,title,price,
substr(cast(date_sub(concat(substr(dsn,1,4),'-',substr(dsn,5,2),'-',substr(dsn,7,2)),7)as string),1,10) as buy_date,
brand_name,cat_id,root_cat_name,`location`,
case when shop_id = '67597230' then 'tmcs' else bc_type end as item_type
from wl_base.t_base_ec_record_dev_new where ds = 'true' and dsn > '20150000' and dsn < '20170400'
AND
user_id in (select user_id from wl_service.t_wrt_airui_kouhong_userid)
)t2
ON
cast(t1.cate_id as int) = cast (t2.cat_id as int);


drop table wl_service.t_wrt_airui_kouhong_profile;
create table wl_service.t_wrt_airui_kouhong_profile AS
select
tb_id,
case when qq_age > 18 and qq_age < 60 then qq_age  else xianyu_birthday end as age,
case when qq_gender = '-' then xianyu_gender else qq_gender end as gender,
tel_loc as loc
from
wl_base.t_base_user_profile_telindex
where tb_id in (select user_id from wl_service.t_wrt_airui_kouhong_userid);

