avg(price) + 3 * std(price) from wl_base.t_base_ec_item_dev_new where ds = 20170110

create table wlservice.tb_item_wrongprice as
select root_cat_id,avg(price) + 3 * std(price) as wrong_price
 from
 wl_base.t_base_ec_item_dev_new where ds = 20170110 group by root_cat_id;

create table wlservice.xy_item_wrongprice as
select categoryid,avg(price) + 3 * std(price) as wrong_price
 from
 wlbase_dev.t_base_ec_xianyu_iteminfo where ds = 20170106 group by categoryid;



select
t1.*
from
t_base_ec_xianyu_iteminfo t1
join
(
select  avg(price) as price_mean ,std(price) price_std ,categoryid from
t_base_ec_xianyu_iteminfo where ds=
group by categoryid
)t2
on t1.categoryid = t2.categoryid
where t1.price > (price_mean+3*price_std)
limit 10000

create table wlservice.tb_wrongprice_iteminfo
select
t1.* FROM
(
select *
FROM
wl_base.t_base_ec_item_dev_new where ds = 20170110)t1
join
tb_item_wrongprice t2
ON
t1.root_cat_id = t2.root_cat_id;
