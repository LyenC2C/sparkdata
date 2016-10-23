


--  inner join 266352505

Drop table t_zlj_t_base_uid_tmp_rank;
create table t_zlj_t_base_uid_tmp_rank as
SELECT

  id1 as id ,uid , ROW_NUMBER() OVER (PARTITION BY uid ORDER BY CAST( regexp_replace(regtime,'.','') as bigint )  DESC) AS rn
from
(
SELECT uid ,id1  from
t_base_uid_tmp where  uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}'
and ds='ttinfo'
)t1
left join t_base_user_profile t2 on t1.id1=t2.tb_id
 ;

-- 328253366
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo' ;
--325857943
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo'  and uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}';


--  提取这批手机号对应数据
create table t_rong360_distinct_user_uid_rank as
 SELECT id,phone_no,rn  from
t_zlj_t_base_uid_tmp_rank t3
join t_rong360_distinct_user t4 on t3.uid =t4.phone_no ;

SELECT *  from t_rong360_distinct_user_uid_rank  where phone_no=13007141774
-- 提取record
Drop table  t_base_ec_record_dev_new_rong360 ;
create table t_base_ec_record_dev_new_rong360 as
SELECT
/*+ mapjoin(t1)*/
t2.*,t1.*
from  t_rong360_distinct_user_uid_rank  t1 join
t_base_ec_record_dev_new t2 on t2.ds='true'  and t1.id =t2.user_id;



-- 提取特征
DROP  table t_base_ec_record_dev_new_rong360_feature_zlj ;
create table  t_base_ec_record_dev_new_rong360_feature_zlj as
SELECT
phone_no,
max(user_id) as user_id ,
COUNT(1) as local_buycount ,
sum(price) as total_price,
sum( case when root_cat_id IN  (  26, 50016768, 50024971, 124470006  ) then 1 else 0 end) as car_flag ,
sum( case when root_cat_id IN  ( 27,  50008164,50020332,50020808, 50022649,50022703, 50022987,50023804,50025881, 122852001,123302001,124698018  ) then 1 else 0 end)
as house_flag ,
sum( case when root_cat_id IN  (25 ,50008165,122650005  ) then 1 else 0 end) as child_flag ,
sum( case when root_cat_id IN  (29 ) then 1 else 0 end) as pet_flag ,
  sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)             annoy_num,
  sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)        /COUNT(1) as  annoy_ratio,
   COUNT(DISTINCT brand_id)    brand_id_num,
   COUNT(DISTINCT root_cat_id) root_cat_id_num,
   sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END)                 b_bc_type_num,
   sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END)/COUNT(1)           b_bc_type_num_ratio,
   sum(CASE WHEN bc_type = 'B'       THEN price         ELSE 0 END)/sum(price)     b_bc_price_ratio,
   sum(CASE WHEN CAST (brand_id as bigint )>10       THEN price         ELSE 0 END)/sum(price)   brand_effec_price_ratio,
   sum(CASE WHEN CAST (brand_id as bigint )>10     THEN 1        ELSE 0 END)/COUNT(1)            brand_effec_num_ratio,
   sum(case when price <=50 then 1 else 0 end)/count(*) as b50_num_ratio,
   sum(case when price <=50 then price else 0 end)/sum(price ) as b50_ratio
from t_base_ec_record_dev_new_rong360 where rn<4 and price<59999
group by phone_no ;


SELECT   *
from t_base_ec_record_dev_new_rong360 where rn<4 and price<59999
and phone_no='13007141774' ;

group by phone_no ;



create table  t_base_ec_record_dev_new_rong360_feature_zlj as
SELECT
phone_no,
sum(case when rn =1 then user_id else 0 end ) as user_id ,

from t_base_ec_record_dev_new_rong360 where rn<4 and price<59999
group by phone_no ;


-- 特征最终表
create table t_base_ec_record_dev_new_rong360_feature_train as

SELECT
 t1.* ,
 t2.
alipay  ,
buycnt  ,
verify  ,
regtime ,
qq_age  ,
qq_gender
 from

t_base_ec_record_dev_new_rong360_feature_zlj t1 join
t_base_user_profile t2 on t1.user_id =t2.user_id
 join  on t1.phone_no=t3.phone_no;


-- ks 计算



SELECT
 p10,
 good_p/good_tag- bad_p/bad_tag

 from
(
SELECT
cast(p*10 as int ) as p10,
sum( case when tag=1 then 1 else 0  end ) as  good_p ,
sum( case when tag=0 then 1 else 0  end ) as  bad_p ,
t_base_ec_record_dev_new_rong360_feature_rs
from
group by cast(p*10 as int )
)t1

join
(
SELECT  from
sum(case when label=1 then 1 else 0 ) as good_tag,
sum(case when label=0 then 1 else 0 ) as bad_tag,
from wlfinance.t_hx_rong360_user  where class='tag_user'  group by label
)t2

