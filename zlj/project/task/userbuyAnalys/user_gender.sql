



create table t_zlj_gender_traindata_userbuy as
select
 /*+ mapjoin(t3)*/
 t2.root_cat_id ,t2.user_id,brand_id,cat_id,gender_id,bc_type,price
 FROM
(select gender_id,tbuid as user_id  from t_zlj_tmp_qqlink_info where gender_id is not null and LENGTH(birthday)>8)t1
join

t_zlj_t_base_ec_item_feed_dev_2015_iteminfo t2
on t1.user_id =t2.user_id;



create table t_zlj_gender_traindata_userbuy_info as
select gender_id,concat_ws(' ', collect_set(tag)) as tags ,
cast(sum(sump) as int)as sump,
cast(avg(avgp) as int) as avgp,
cast(max(maxp) as int) as maxp,
cast(min(minp) as int) as minp
from
(
SELECT
  user_id,
  gender_id,
  concat( root_cat_id,":",cast(count(1) as string)) tag,sum(price) sump,avg(price) avgp,cast(max(price) as int) maxp ,cast(min(price) as int) minp
FROM t_zlj_gender_traindata_userbuy group by  user_id,root_cat_id ,gender_id
)t group by user_id,gender_id ;



create table t_zlj_gender_predict_data_userbuy_info as
select user_id,concat_ws(' ', collect_set(tag)) as tags ,
cast(sum(sump) as int) as sump,
cast(avg(avgp) as int) as avgp,
cast(max(maxp) as int) as maxp,
cast(min(minp) as int) as minp
from
(
SELECT
  user_id,
  concat( root_cat_id,":",cast(count(1) as string)) tag,sum(price) sump,avg(price) avgp,cast(max(price) as int) maxp ,cast(min(price) as int) minp
FROM t_zlj_t_base_ec_item_feed_dev_2015_iteminfo group by  user_id,root_cat_id
)t group by user_id ;


-- create table t_zlj_gender_traindata_userbuy_info_rootcat_brand as
-- select gender_id,concat_ws(' ', collect_set(tag)) as tags ,
--
-- cast(sum(sump) as int)as sump,
-- cast(avg(avgp) as int) as avgp
-- from
-- (
-- SELECT
--   user_id,
--   gender_id,
--   concat( root_cat_id,":",cast(count(1) as string)) tag,sum(price) sump,avg(price) avgp
-- FROM t_zlj_gender_traindata_userbuy group by  user_id,root_cat_id ,gender_id
--
-- union all
--
-- SELECT
--   user_id,
--   gender_id,
--   concat( brand_id,":",cast(count(1) as string)) tag,sum(price) sump,avg(price) avgp
-- FROM t_zlj_gender_traindata_userbuy group by  user_id,brand_id ,gender_id
--
-- )t group by user_id,gender_id ;