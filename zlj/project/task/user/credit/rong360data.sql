





-- 328253366
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo' ;
--325857943
SELECT COUNT(1) from t_base_uid_tmp where ds='ttinfo'  and uid  rlike '^1(3[0-9]|4[57]|5[0-35-9]|7[01678]|8[0-9])\\d{8}';


--step1  提取这批手机号对应数据

Drop table t_rong360_distinct_user_uid_rank;
create table t_rong360_distinct_user_uid_rank as
 SELECT id,phone_no,rn  from
t_zlj_t_base_uid_tmp_rank t3
join (SELECT DISTINCT phone_no from wlfinance.t_hx_rong360_user ) t4 on t3.uid =t4.phone_no ;


-- test_1w 关联数据量
SELECT COUNT(1) from t_rong360_distinct_user_uid_rank t1 join  wlfinance.t_hx_rong360_user t2 on t1.phone_no=t2.phone_no and t2.class='test_1w'

-- step2  提取record 所有原始数据
Drop table  t_base_ec_record_dev_new_rong360 ;
create table t_base_ec_record_dev_new_rong360 as
SELECT
/*+ mapjoin(t1)*/
t2.*,t1.*
from  t_rong360_distinct_user_uid_rank  t1 join
t_base_ec_record_dev_new t2 on t2.ds='true'  and t1.id =t2.user_id;

SELECT  label ,COUNT(1) from
(
select t2.phone_no, label from t_base_ec_record_dev_new_rong360 t1 join  wlfinance.t_hx_rong360_user t2 on t1.phone_no=t2.phone_no and t2.class='test_1w'
group by t2.phone_no, label
)t group by label ;
-- step3 zlj 提取特征
DROP  table t_base_ec_record_dev_new_rong360_feature_zlj ;
create table  t_base_ec_record_dev_new_rong360_feature_zlj as
SELECT
phone_no as id ,
phone_no,
max(user_id) as user_id ,
COUNT(1) as local_buycount ,
sum(price) as total_price,
sum( case when root_cat_id IN  ( 26, 50016768, 50024971, 124470006  ) then 1 else 0 end) as car_flag ,
sum( case when root_cat_id IN  ( 27,  50008164,50020332,50020808, 50022649,50022703, 50022987,50023804,50025881, 122852001,123302001,124698018  ) then 1 else 0 end)
as house_flag ,
sum( case when root_cat_id IN  (25 ,50008165,122650005  ) then 1 else 0 end) as child_flag ,
sum( case when root_cat_id IN  (29 ) then 1 else 0 end) as pet_flag ,
  sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)             annoy_num,
  sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)/COUNT(1) as  annoy_ratio,
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
group by phone_no
;
id	            string
phone_no	      string	手机号码
user_id	        string	淘宝id
local_buycount	bigint	购买总记录
total_price	    double	购买总金额
car_flag	      bigint	是否有车
house_flag	    bigint	是否有房
child_flag	    bigint	是否有小孩
pet_flag       	bigint	是否养宠物
annoy_num	      bigint	匿名评论数量
annoy_ratio	      double	匿名评论占比
brand_id_num	          bigint	购买总品牌数
root_cat_id_num    	    bigint	购买一级类目数
b_bc_type_num	          bigint	购买B店次数
b_bc_type_num_ratio   	double	购买B店次数占比
b_bc_price_ratio	      double	购买B店金额占比
brand_effec_price_ratio	double	有效品牌购买总金额占比
brand_effec_num_ratio	  double	有效品牌购买总次数占比
b50_num_ratio	          double	五十元以下购买总次数
b50_ratio	              double	五十元以下购买金额占比




--
SELECT   *
from t_base_ec_record_dev_new_rong360 where rn<4 and price<59999
and phone_no='13007141774'  ;

group by phone_no ;




--step4
-- Drop table t_base_ec_record_dev_new_rong360_feature_train_allclass;
-- create table t_base_ec_record_dev_new_rong360_feature_train_allclass as
-- SELECT
-- t1.id   ,
-- label ,
-- class,
-- local_buycount           ,
-- total_price              ,
-- car_flag                 ,
-- house_flag               ,
-- child_flag               ,
-- pet_flag                 ,
-- annoy_num                ,
-- annoy_ratio              ,
-- brand_id_num             ,
-- root_cat_id_num          ,
-- b_bc_type_num            ,
-- b_bc_type_num_ratio      ,
-- b_bc_price_ratio         ,
-- brand_effec_price_ratio  ,
-- brand_effec_num_ratio    ,
-- b50_num_ratio            ,
-- b50_ratio               ,
--  t3.*  ,
-- case when alipay like '已通过支付宝实名认证' then 1 else 0 end  as alipay_flag  ,
-- cast(buycnt as int)+0 as buycut  ,
-- case when cast(regexp_replace(verify, 'VIP等级', '')  as int) is null  then -1 else cast(regexp_replace(verify, 'VIP等级', '')  as int) end    as  verify_level ,
-- (12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-')))) regtime_month ,
-- case when cast(qq_gender as int) is NULL then -1 else cast(qq_gender as int) end   as qq_gender
--  from
-- t_base_ec_record_dev_new_rong360_feature_zlj t1 join
-- t_base_user_profile t2 on t1.user_id =t2.tb_id
--  join  wlfinance.t_hx_model_rong360_finnal t3  on t1.phone_no=t3.phone_no
--  join wlfinance.t_hx_rong360_user  t4  on t1.phone_no=t4.phone_no ;

-- SELECT qq_gender ,cast(qq_gender as int)+0,cast(qq_gender as int)  from t_base_user_profile limit 10;
-- step5 特征最终表
Drop table t_base_ec_record_dev_new_rong360_feature_train;
create table t_base_ec_record_dev_new_rong360_feature_train as
SELECT
t1.id   ,
class ,
label ,
local_buycount           ,
total_price              ,
car_flag                 ,
house_flag               ,
child_flag               ,
pet_flag                 ,
annoy_num                ,
annoy_ratio              ,
brand_id_num             ,
root_cat_id_num          ,
b_bc_type_num            ,
b_bc_type_num_ratio      ,
b_bc_price_ratio         ,
brand_effec_price_ratio  ,
brand_effec_num_ratio    ,
b50_num_ratio            ,
b50_ratio               ,
 t3.*  ,
case when alipay like '已通过支付宝实名认证' then 1 else 0 end  as alipay_flag  ,
buycnt  ,
cast(regexp_replace(verify, 'VIP等级', '')  as int) as  verify_level ,
verify,
(12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-')))) regtime_month ,
cast(qq_gender as int) as qq_gender
 from
t_base_ec_record_dev_new_rong360_feature_zlj t1 join
t_base_user_profile t2 on t1.user_id =t2.tb_id
 join  wlfinance.t_hx_model_rong360_finnal2 t3  on t1.phone_no=t3.phone_no
  join wlfinance.t_hx_rong360_user  t4  on t1.phone_no=t4.phone_no  ;


SELECT  count(1) from wlfinance.t_hx_rong360_user where class in  ( 'test_1w' ,'tag_user');

-- SELECT cast(regexp_replace(verify, 'VIP等级', '')  as int) as  verify_level from t_base_user_profile limit 10
--
-- create table wlfinance.t_zlj_tmp_uid like wlfinance.t_hx_rong360_user

--

-- 获取所有特征
SELECT  class,label ,gender,age,t4.*
from
(
SELECT tel ,tel_index,class,label ,gender,age from
(
SELECT  tel,'8000_c' as class , id1 as label,id3 as gender,id4 as age
   from
   wlfinance.t_zlj_base_match where ds='ygz_part'
   union all
    SELECT tel,'2000_c' as class,'' as label ,id1 as gender,id2 as age
   from
   wlfinance.t_zlj_base_match where ds='rong360_test_1111'
    union all
   SELECT  tel,'data_2k' as class , id1 as label,id3 as gender,id4 as age
   from
   wlfinance.t_zlj_base_match where ds='data_2k'
    )t1
    join
t_zlj_phone_rank_index t2 on t1.tel =t2.uid
group by tel_index,class,label ,gender,age,t1.tel
)t3 join
wlservice.t_rong360_model_features_new  t4 on t3.tel=t4.tel ;