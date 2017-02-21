#!/usr/bin/env bash

hive<<EOF
set hive.merge.mapfiles=true;
create table if not exists wl_base.t_wrt_caiji_search_newitemid_update
as
select t1.id
from
(select nid from t_base_item_search where ds = 20170217) t1
left join
(select item_id t_base_ec_item_dev_new where ds = 20170217) t2
on t1.id =t2.id

EOF
