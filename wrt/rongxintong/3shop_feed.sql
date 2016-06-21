#!/usr/bin/env bash

hive<<EOF

use wlservice;

create table t_wrt_104820621_feed as
select 
/*+ mapjoin(t) */
t_base_ec_item_feed_dev.* from 
wlbase_dev.t_base_ec_item_feed_dev
join
(select item_id from wlbase_dev.t_base_ec_item_dev_new where shop_id = 104820621 and ds = 20160615)t
on
t_base_ec_item_feed_dev.item_id = t.item_id;

create table t_wrt_57299948_feed as
select 
/*+ mapjoin(t) */
t_base_ec_item_feed_dev.* from 
wlbase_dev.t_base_ec_item_feed_dev
join
(select item_id from wlbase_dev.t_base_ec_item_dev_new where shop_id = 57299948 and ds = 20160615)t
on
t_base_ec_item_feed_dev.item_id = t.item_id;

create table t_wrt_103569798_feed as
select 
/*+ mapjoin(t) */
t_base_ec_item_feed_dev.* from 
wlbase_dev.t_base_ec_item_feed_dev
join
(select item_id from wlbase_dev.t_base_ec_item_dev_new where shop_id = 103569798 and ds = 20160615)t
on
t_base_ec_item_feed_dev.item_id = t.item_id;

EOF
