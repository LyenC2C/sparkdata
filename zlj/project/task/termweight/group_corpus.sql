
create table t_zlj_userbuy_item_tfidf_tagbrand_weight_2015_v1_user_group as
select
user_id,
concat_ws('\003', collect_set(title_cut)) as title_cut_all
from
t_zlj_userbuy_item_tfidf_tagbrand_weight_2015_v1

group by user_id ;