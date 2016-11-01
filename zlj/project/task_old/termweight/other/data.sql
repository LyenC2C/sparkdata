
drop table if EXISTS  t_zlj_tmp;

create table t_zlj_tmp AS

SELECT
item_id ,title,root_cat_id,root_cat_name
FROM

t_base_ec_item_dev
where  ds=20151107 and root_cat_id in
(
50011699,
30,
16,
1625,
50011699
) ;


-- import  temp file
create table t_zlj_feed_tag_0901 as
select

user_id, concat_ws('\003', collect_set(hmm)) as hmm

from
(
    select item_id,concat_ws('\002',title_cut_stag,concat(cat_name,'-c_n'), concat(brand_name,'-b_n'))  as hmm
     from t_base_ec_item_title_cut_with_brand_tag_c
     where LENGTH(item_id)>0
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev
where ds>20150901
and  LENGTH (user_id)>0 and  LENGTH (item_id)>0
group by item_id,user_id
)t2
on t1.item_id=t2.item_id
group by user_id ;


create table t_zlj_feed_tag_0701 as
select
user_id, concat_ws('\003', collect_set(hmm)) as hmm
from
(
    select item_id,concat_ws('\002',title_cut_stag,concat(cat_name,'-c_n'), concat(brand_name,'-b_n'))  as hmm
     from t_base_ec_item_title_cut_with_brand_tag_c
     where LENGTH(item_id)>0
)t1
join
(
select item_id,user_id from t_base_ec_item_feed_dev
where ds>20150701
and  LENGTH (user_id)>0 and  LENGTH (item_id)>0
group by item_id,user_id
)t2
on t1.item_id=t2.item_id
group by user_id ;