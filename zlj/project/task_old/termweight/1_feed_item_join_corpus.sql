

/home/hadoop/hive/bin/hive<<EOF


use wlbase_dev;

drop table if EXISTS  t_zlj_item_feed_title_cut_20151226;

create table t_zlj_item_feed_title_cut_20151226 as
select user_id,title_cut
from
(

select item_id,title_cut from t_zlj_base_ec_item_title_cut where ds=20160216

)t1
join
(
select item_id,user_id from t_base_ec_record_dev_s
group by item_id,user_id
)
t2
on t1.item_id=t2.item_id ;


EOF

-- create table t_zlj_tmp as  select * from t_base_ec_item_title_cut where ds=20151225 limit 5
-- ;

