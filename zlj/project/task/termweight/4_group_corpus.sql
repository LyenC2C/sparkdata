

/home/hadoop/hive/bin/hive<<EOF


use wlbase_dev;
drop table if EXISTS  t_zlj_userbuy_item_corpus_2015_user_group;


create table t_zlj_userbuy_item_corpus_2015_user_group as
select
user_id,
concat_ws('\003', collect_set(title_cut)) as title_cut_all
from
t_zlj_userbuy_item_corpus_2015

group by user_id ;

EOF