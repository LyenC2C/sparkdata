#!/usr/bin/env bash

hive<<EOF

use wlservice;

create table t_wrt_104820621_monsold as 
select t1.mon,sum(t1.day_sold) from
(select item_id,day_sold,substr(ds,1,6) as mon from wlbase_dev.t_base_ec_item_daysale_dev_new )t1
join
(select item_id,shop_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615 and shop_id = 104820621)t2
on
t1.item_id = t2.item_id
group by t1.mon;

create table t_wrt_57299948_monsold as 
select t1.mon,sum(t1.day_sold) from
(select item_id,day_sold,substr(ds,1,6) as mon from wlbase_dev.t_base_ec_item_daysale_dev_new )t1
join
(select item_id,shop_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615 and shop_id = 57299948)t2
on
t1.item_id = t2.item_id
group by t1.mon;

create table t_wrt_103569798_monsold as 
select t1.mon,sum(t1.day_sold) from
(select item_id,day_sold,substr(ds,1,6) as mon from wlbase_dev.t_base_ec_item_daysale_dev_new )t1
join
(select item_id,shop_id from wlbase_dev.t_base_ec_item_dev_new where ds = 20160615 and shop_id = 103569798)t2
on
t1.item_id = t2.item_id
group by t1.mon;


EOF
