-- 重复购买率


create table wlservice.t_zlj_record_recycle_check as
select
 user_id, item_id,cate_level1_id, cate_level2_id, cate_level3_id, cate_level4_id, cate_level5_id,shop_id ,count(1) as times
from

t_base_record_cate_simple_ds

group by user_id, item_id,cate_level1_id, cate_level2_id, cate_level3_id, cate_level4_id, cate_level5_id,shop_id;


create table wlservice.t_zlj_record_recycle_check_item_score as
select
user_id ,sum(log(`_c8`)) as recycle_score
from   wlservice.t_zlj_record_recycle_check
where `_c8`>1
group by user_id  ;


--0.20205
--1477879 count()
select
--corr(recycle_score, cast(t2.id2 as int ))
count(1)
from
wlservice.t_zlj_record_recycle_check_item_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima'


create table wlservice.t_zlj_record_recycle_check_shop_score as
select
user_id ,sum(log(shop_sum)) as recycle_score
from
(
select user_id ,shop_id ,sum(`_c8`) as shop_sum
from   wlservice.t_zlj_record_recycle_check
group by user_id ,shop_id
  )t group by user_id;


  ---0.2922
  ---1561510  count()
  select
--corr(recycle_score, cast(t2.id2 as int ))
count(1)
from
wlservice.t_zlj_record_recycle_check_shop_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima'



 create table wlservice.t_zlj_record_recycle_check_cate_level1_id_score as
select
user_id ,sum(log(shop_sum)) as recycle_score
from
(
select user_id ,cate_level1_id ,sum(`_c8`) as shop_sum
from   wlservice.t_zlj_record_recycle_check
group by user_id ,cate_level1_id
  )t group by user_id;



   --- -0.33542
  ---1561510  count()
  select
  --corr(recycle_score, cast(t2.id2 as int ))
count(1)
from
wlservice.t_zlj_record_recycle_check_cate_level1_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima'





  create table wlservice.t_zlj_record_recycle_cate_level2_id_id_score as
select
user_id ,sum(log(shop_sum)) as recycle_score
from
(
select user_id  ,sum(`_c8`) as shop_sum
from   wlservice.t_zlj_record_recycle_check
  where cate_level2_id is not null
group by user_id ,cate_level2_id
  )t group by user_id;


   --- 0.2980
  ---1561510  count()
  select
  corr(recycle_score, cast(t2.id2 as int ))
--count(1)
from
wlservice.t_zlj_record_recycle_cate_level2_id_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima'



   create table wlservice.t_zlj_record_recycle_cate_level3_id_id_score as
select
user_id ,sum(log(shop_sum)) as recycle_score
from
(
select user_id  ,sum(`_c8`) as shop_sum
from   wlservice.t_zlj_record_recycle_check
  where cate_level3_id is not null
group by user_id ,cate_level3_id
  )t group by user_id;


     --- 0.2960
  ---1561510  count()
  select
  corr(recycle_score, cast(t2.id2 as int ))
--count(1)
from
wlservice.t_zlj_record_recycle_cate_level3_id_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima';



   create table wlservice.t_zlj_record_recycle_cate_level4_id_id_score as
select
user_id ,sum(log(shop_sum)) as recycle_score
from
(
select user_id  ,sum(`_c8`) as shop_sum
from   wlservice.t_zlj_record_recycle_check
  where cate_level4_id is not null
group by user_id ,cate_level4_id
  )t group by user_id;


   ---  0.2404
  ---1561510  count()
  select
  corr(recycle_score, cast(t2.id2 as int ))
--count(1)
from
wlservice.t_zlj_record_recycle_cate_level4_id_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima';



   create table wlservice.t_zlj_record_recycle_cate_level5_id_id_score as
select
user_id ,sum(log(shop_sum)) as recycle_score
from
(
select user_id  ,sum(`_c8`) as shop_sum
from   wlservice.t_zlj_record_recycle_check
  where cate_level5_id is not null
group by user_id ,cate_level5_id
  )t group by user_id;


   ---  0.2404
  ---1561510  count()
  select
  corr(recycle_score, cast(t2.id2 as int ))
--count(1)
from
wlservice.t_zlj_record_recycle_cate_level5_id_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima';


--一级类目特征 相关性 	0.69839709362736979
select
 corr(recycle_score, cast(buycnt as int) )
from
t_base_user_profile t1
join
(
  select user_id ,recycle_score
  from
wlservice.t_zlj_record_recycle_check_cate_level1_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima')  t2  on t1.tb_id =t2.user_id ;


-- 0.7164
select
 corr(recycle_score, cast(buycnt as int) )
from
t_base_user_profile t1
join
(
  select user_id ,recycle_score
  from
wlservice.t_zlj_record_recycle_cate_level2_id_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima')  t2  on t1.tb_id =t2.user_id ;

-- 0.6897
select
 corr(recycle_score, cast(buycnt as int) )
from
t_base_user_profile t1
join
(
  select user_id ,recycle_score
  from
wlservice.t_zlj_record_recycle_cate_level3_id_id_score  t1  join
wlfinance.t_zlj_base_match  t2 on t1.user_id =t2.id1 and t2.ds='zhima')  t2  on t1.tb_id =t2.user_id ;



create table wlservice.t_zlj_record_recycle_check_merge_score as
select
t1.user_id,
t1.recycle_score as item_recycle_score ,
t2.recycle_score as shop_recycle_score ,
t3.recycle_score as level1_recycle_score ,
t4.recycle_score as level2_recycle_score ,
t5.recycle_score as level3_recycle_score ,
t6.recycle_score as level4_recycle_score
from
wlservice.t_zlj_record_recycle_check_item_score  t1
left join
wlservice.t_zlj_record_recycle_check_shop_score  t2  on t1.user_id=t2.user_id
left  join
wlservice.t_zlj_record_recycle_check_cate_level1_id_score t3 on t1.user_id=t3.user_id
left  join
wlservice.t_zlj_record_recycle_cate_level2_id_id_score  t4  on t1.user_id=t4.user_id
left  join
wlservice.t_zlj_record_recycle_cate_level3_id_id_score  t5   on t1.user_id=t5.user_id
left  join
wlservice.t_zlj_record_recycle_cate_level4_id_id_score t6 on t1.user_id=t6.user_id ;