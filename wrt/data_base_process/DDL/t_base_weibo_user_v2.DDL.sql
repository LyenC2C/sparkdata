use wlservice;
create table t_tmp_wrt_weibo_user
(
id string,
screen_name string,
location string,
description string,
domain string,
weihao string,
gender string,
followers_count string,
friends_count string,
statuses_count string,
favourites_count string,
created_at string,
geo_enabled string,
verified string,
verified_type string,
avatar_large string,
verified_reason string,
bi_followers_count string
)
COMMENT '临时微博用户有用信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'   LINES TERMINATED BY '\n';


insert overwrite table wlservice.t_tmp_wrt_weibo_user
select
regexp_replace(id,'\t',''),
regexp_replace(screen_name,'\t',''),
regexp_replace(location,'\t',''),
regexp_replace(description,'\t',''),
regexp_replace(domain,'\t',''),
regexp_replace(weihao,'\t',''),
regexp_replace(gender,'\t',''),
regexp_replace(followers_count,'\t',''),
regexp_replace(friends_count,'\t',''),
regexp_replace(statuses_count,'\t',''),
regexp_replace(favourites_count,'\t',''),
regexp_replace(created_at,'\t',''),
regexp_replace(geo_enabled,'\t',''),
regexp_replace(verified,'\t',''),
regexp_replace(verified_type,'\t',''),
regexp_replace(avatar_large,'\t',''),regexp_replace(verified_reason,'\t',''),
regexp_replace(verified_reason,'\t',''),regexp_replace(follow_me,'\t',''),
regexp_replace(bi_followers_count,'\t',''),regexp_replace(lang,'\t',''),
from wlbase_dev.t_base_weibo_user


insert overwrite table t_tmp_wrt_weibo_user
select
regexp_replace(id,'\t',''),
regexp_replace(screen_name,'\t',''),
regexp_replace(location,'\t',''),
regexp_replace(description,'\t',''),
regexp_replace(domain,'\t',''),
regexp_replace(weihao,'\t',''),
regexp_replace(gender,'\t',''),
regexp_replace(followers_count,'\t',''),
regexp_replace(friends_count,'\t',''),
regexp_replace(statuses_count,'\t',''),
regexp_replace(favourites_count,'\t',''),
regexp_replace(created_at,'\t',''),
regexp_replace(geo_enabled,'\t',''),
regexp_replace(verified,'\t',''),
regexp_replace(verified_type,'\t',''),
regexp_replace(avatar_hd,'\t',''),
regexp_replace(follow_me,'\t',''),
regexp_replace(lang,'\t','')
from wlbase_dev.t_base_weibo_user