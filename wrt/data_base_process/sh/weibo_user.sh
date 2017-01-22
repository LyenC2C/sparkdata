#!/usr/bin/env bash

#!/usr/bin/env bash
source ~/.bashrc
pre_path='/home/wrt/sparkdata'

hfs -rmr /user/wrt/temp/userinfo_all

spark-submit  --executor-memory 9G  --driver-memory 9G  --total-executor-cores 120 \
$pre_path/wrt/data_base_process/weibo/t_base_weibo_user.py

hive<<EOF

use wlbase_dev;

LOAD DATA   INPATH '/user/wrt/temp/userinfo_all' OVERWRITE INTO TABLE t_base_weibo_user_new PARTITION (ds='temp')

insert OVERWRITE table t_base_weibo_user_new PARTITION(ds = '20161212')
select
case when t1.id is null then t2.id else t1.id end,
case when t1.idstr is null then t2.idstr else t1.idstr end,
case when t1.screen_name is null then t2.screen_name else t1.screen_name end,
case when t1.name is null then t2.name else t1.name end,
case when t1.province is null then t2.province else t1.province end,
case when t1.city is null then t2.city else t1.city end,
case when t1.location is null then t2.location else t1.location end,
case when t1.description is null then t2.description else t1.description end,
case when t1.url is null then t2.url else t1.url end,
case when t1.profile_image_url is null then t2.profile_image_url else t1.profile_image_url end,
case when t1.profile_url is null then t2.profile_url else t1.profile_url end,
case when t1.domain is null then t2.domain else t1.domain end,
case when t1.weihao is null then t2.weihao else t1.weihao end,
case when t1.gender is null then t2.gender else t1.gender end,
case when t1.followers_count is null then t2.followers_count else t1.followers_count end,
case when t1.friends_count is null then t2.friends_count else t1.friends_count end,
case when t1.statuses_count is null then t2.statuses_count else t1.statuses_count end,
case when t1.favourites_count is null then t2.favourites_count else t1.favourites_count end,
case when t1.created_at is null then t2.created_at else t1.created_at end,
case when t1.wfollowing is null then t2.wfollowing else t1.wfollowing end,
case when t1.allow_all_act_msg is null then t2.allow_all_act_msg else t1.allow_all_act_msg end,
case when t1.geo_enabled is null then t2.geo_enabled else t1.geo_enabled end,
case when t1.verified is null then t2.verified else t1.verified end,
case when t1.verified_type is null then t2.verified_type else t1.verified_type end,
case when t1.remark is null then t2.remark else t1.remark end,
case when t1.allow_all_comment is null then t2.allow_all_comment else t1.allow_all_comment (end,
case when t1.avatar_large is null then t2.avatar_large else t1.avatar_large end,
case when t1.avatar_hd is null then t2.avatar_hd else t1.avatar_hd end,
case when t1.verified_reason is null then t2.verified_reason else t1.verified_reason end,
case when t1.follow_me is null then t2.follow_me else t1.follow_me end,
case when t1.online_status is null then t2.online_status else t1.online_status end,
case when t1.bi_followers_count is null then t2.bi_followers_count else t1.bi_followers_count end,
case when t1.lang is null then t2.lang else t1.lang end
from
(select * from t_base_weibo_user_new where ds = 'temp')t1
full outer JOIN
(select * from t_base_weibo_user_new where ds = '20161123')t2
ON
t1.item_id = t2.item_id;
EOF