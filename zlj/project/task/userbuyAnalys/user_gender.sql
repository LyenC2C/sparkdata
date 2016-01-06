

-- 元数据
create table t_zlj_gender_traindata_userbuy as
select
 /*+ mapjoin(t3)*/
 t2.root_cat_id ,t2.user_id, brand_id,cat_id,bc_type,price,annoy,content_length,

 t1.years,t1.gender_id
 FROM
(select gender_id,tbuid as user_id ,
   year(from_unixtime(unix_timestamp(),'yyyy-MM-dd'))-year(birthday) as years

  from t_zlj_tmp_qqlink_info where gender_id is not null and LENGTH(birthday)>8 and LENGTH(birthday)>6
  )t1
join

t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t t2
on t1.user_id =t2.user_id;

-- 训练数据
create table t_zlj_gender_traindata_userbuy_info_root_cat_id as
select gender_id,concat_ws(' ', collect_set(tag)) as tags
from
(
SELECT
  user_id,
  gender_id,
  concat( root_cat_id,":",cast(count(1) as string)) tag

FROM t_zlj_gender_traindata_userbuy

group by  user_id,root_cat_id ,gender_id
)t group by user_id,gender_id ;





create table t_zlj_gender_traindata_userbuy_info as
select gender_id,concat_ws(' ', collect_set(tag)) as tags ,
sum(sannoy)  sannoy,
sum(no_annoy)  no_annoy,
sum(5_feed_length)  5_feed_length,
sum(10_feed_length)  10_feed_length,
sum(20_feed_length)  20_feed_length,
sum(20up_feed_length)  20up_feed_length
from
(
SELECT
  user_id,
  gender_id,
  concat( cat_id,":",cast(count(1) as string)) tag,
  cast(sum(annoy) as bigint) sannoy,
  sum(case when annoy=0 then 1 else 0 end)as no_annoy,
  sum(case when content_length<=5 then 1 else 0 end) as 5_feed_length,
  sum(case when content_length<=10 and content_length>5 then 1 else 0 end) as 10_feed_length,
  sum(case when content_length<=20 and content_length>10 then 1 else 0 end) as 20_feed_length ,
  sum(case when content_length>20  then 1 else 0 end) as 20up_feed_length
FROM t_zlj_gender_traindata_userbuy group by  user_id,cat_id ,gender_id
)t group by user_id,gender_id ;




create table t_zlj_gender_predict_data_userbuy_info as
select user_id,concat_ws(' ', collect_set(tag)) as tags
from
(
SELECT
  user_id,
  concat( root_cat_id,":",cast(count(1) as string)) tag
FROM t_zlj_t_base_ec_item_feed_dev_2015_iteminfo_t group by  user_id,root_cat_id
)t group by user_id ;



-- age


create table t_zlj_gender_traindata_userbuy_info_age as
select years,concat_ws(' ', collect_set(tag)) as tags
from
(
SELECT
  user_id,
  years,
  concat( root_cat_id,":",cast(count(1) as string)) tag
FROM t_zlj_gender_traindata_userbuy group by  user_id,root_cat_id ,years
)t group by user_id,years;