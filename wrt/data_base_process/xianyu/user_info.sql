--闲鱼用户相关信息挖掘

create table wlservice.t_wrt_xianyu_user_price as
select userid,
round(sum(price),2) as  price_sum,
round(count(1),2) as buy_count,
round(avg(price),2) as price_avg,
round(max(price),2) as price_max,
round(min(price),2) as price_min,
round(std(price),2) as price_std ,
round(percentile(cast(price as int),0.5),2) as price_median,
round(percentile(cast(price as int),0.25),2) as price_025,
round(percentile(cast(price as int),0.10),2) as price_010,
round(percentile(cast(price as int),0.75),2) as price_075,
round(max(price)-min(price),2) as price_cross
from wlbase_dev.t_base_ec_xianyu_iteminfo where ds = 20161221
group by userid;



create table wlservice.t_wrt_xianyu_user_ as
select
userid,
max(iscar) as iscar,
max(ishouse) as ishouse,
max(isgudong) as isgudong,
max(isshechipin) as isshechipin
from
(
select
itemid,
userid,
case when categoryid in ('50025463','125518002') then "1" else "0" end as iscar,
case when categoryid = '125522001' and (title like '%房%' or description like '%房%' or description rlike '\\d+平' )
then "1" else "0" end as ishouse,
case when categoryid in ('125520002','50025448','50025450','50025451') then "1" else "0" end as isgudong,
case when categoryid in ('50232001','50025423','50198001','50025425','50230001','50025422','50214002')
then "1" else "0" END as isshechipin
FROM
wlbase_dev.t_base_ec_xianyu_iteminfo where ds = '20161221'
)t
group by userid;