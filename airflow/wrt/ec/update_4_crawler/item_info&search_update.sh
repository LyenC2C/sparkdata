#!/usr/bin/env bash

beeline -u "jdbc:hive2://cs105:10000/;principal=hive/cs105@HADOOP.COM"<<EOF
set hive.merge.mapfiles= true;
set hive.merge.mapredfiles= true;
set hive.merge.size.per.task=268435456;
set hive.merge.smallfiles.avgsize=16777216;
insert overwrite table wl_analysis.t_wrt_caiji_serach_newid
select t1.nid,t1.ct from
(select nid,cast(comment_count as int) as ct from wl_base.t_base_item_search where ds = '20170217' having ct is not null and ct > 0  )t1
left join
(select item_id from wl_base.t_base_ec_item_dev_new where ds = '20170217')t2
on
t1.nid = t2.item_id
where
t2.item_id is null
order by t1.ct desc;
EOF
