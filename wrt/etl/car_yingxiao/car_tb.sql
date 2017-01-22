create table wlservice.t_wrt_lul_car_tb as
select user_id from
(select item_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20161202 and title like "%试驾%")t1
JOIN
(select user_id,item_id from wlbase_dev.t_base_ec_item_feed_dev_new where ds > 20161100) t2
ON
t1.item_id = t2.item_id
group by user_id;

drop table wlservice.t_wrt_luhu_jiebao_car_tb;
create table wlservice.t_wrt_luhu_jiebao_car_tb as
select user_id,
case
when t1.title like "%路虎%" then "路虎"
else "捷豹"
end as car_type
from
(select item_id,title from wlbase_dev.t_base_ec_item_dev_new where ds = 20161202 and (title like "%试驾%"
and(title like "%路虎%" or title like "%捷豹%")))t1
JOIN
(
select a.user_id,a.item_id from
(select user_id,item_id from wlbase_dev.t_base_ec_item_feed_dev_new where ds > 20160900)a
JOIN
(select tb_id from wlbase_dev.t_base_user_profile where tel_loc like "%浙江%")b
on
a.user_id = b.tb_id
) t2
ON
t1.item_id = t2.item_id
group by
user_id,
case
when t1.title like "%路虎%" then "路虎"
else "捷豹"
end
;

create table wlservice.t_wrt_weiboid_benchi as
select fri.id,new.location from
(select id from wlbase_dev.t_base_weibo_user_fri where ds=20161106 and ids like "%1960399214%") fri
join
(select id,location from wlbase_dev.t_base_weibo_user_new where ds=20161123) new
on fri.id = new.id;


create table wlservice.t_wrt_tb_benchi as
select user_id from
(select item_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20161202 and title like "%试驾%")t1
JOIN
(select user_id,item_id from wlbase_dev.t_base_ec_item_feed_dev_new where ds > 20160900) t2
ON
t1.item_id = t2.item_id
group by user_id;

