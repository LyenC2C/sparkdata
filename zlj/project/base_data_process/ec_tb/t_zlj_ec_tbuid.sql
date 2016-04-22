



use wlbase_dev;

drop TABLE  if EXISTS  t_zlj_ec_tbuid;

create table t_zlj_ec_tbuid as

SELECT

user_id

from
t_base_ec_item_feed_dev
where ds >20130101
group by user_id ;