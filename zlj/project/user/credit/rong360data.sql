


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
phone_no as id ,
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



--
SELECT   *
from t_base_ec_record_dev_new_rong360 where rn<4 and price<59999
and phone_no='13007141774'  ;

group by phone_no ;




--
Drop table t_base_ec_record_dev_new_rong360_feature_train_allclass;
create table t_base_ec_record_dev_new_rong360_feature_train_allclass as
SELECT
t1.id   ,
label ,
class,
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
cast(buycnt as int)+0 as buycut  ,
case when cast(regexp_replace(verify, 'VIP等级', '')  as int) is null  then -1 else cast(regexp_replace(verify, 'VIP等级', '')  as int) end    as  verify_level ,
(12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-')))) regtime_month ,
case when cast(qq_gender as int) is NULL then -1 else cast(qq_gender as int) end   as qq_gender
 from
t_base_ec_record_dev_new_rong360_feature_zlj t1 join
t_base_user_profile t2 on t1.user_id =t2.tb_id
 join  wlfinance.t_hx_model_rong360_finnal t3  on t1.phone_no=t3.phone_no
 join wlfinance.t_hx_rong360_user  t4  on t1.phone_no=t4.phone_no ;

-- SELECT qq_gender ,cast(qq_gender as int)+0,cast(qq_gender as int)  from t_base_user_profile limit 10;
-- 特征最终表
Drop table t_base_ec_record_dev_new_rong360_feature_train;
create table t_base_ec_record_dev_new_rong360_feature_train as
SELECT
t1.id   ,
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
cast(regexp_replace('verify', 'VIP等级', '')  as int) as  verify_level ,
(12 * (2016 - YEAR(regexp_replace(regtime, '\\.', '-'))) + (7 - MONTH(regexp_replace(regtime, '\\.', '-')))) regtime_month ,
cast(qq_gender as int) as qq_gender
 from
t_base_ec_record_dev_new_rong360_feature_zlj t1 join
t_base_user_profile t2 on t1.user_id =t2.tb_id
 join  wlfinance.t_hx_model_rong360_finnal t3  on t1.phone_no=t3.phone_no
 join wlfinance.t_hx_rong360_user  t4  on t1.phone_no=t4.phone_no and t4.class='tag_user';

--
0       3383
1       232
SELECT label,COUNT (1) from t_base_ec_record_dev_new_rong360_feature_train group by label;


SELECT COUNT(DISTINCT  phone_no ) from t_base_ec_record_dev_new_rong360_feature_train ;
-- ks 计算



SELECT
 good_p1 ,bad_p1 ,good_p1/good_tag ,bad_p1/bad_tag ,good_p1/good_tag- bad_p1/bad_tag ,
 good_p2 ,bad_p2 ,good_p2/good_tag ,bad_p2/bad_tag ,good_p2/good_tag- bad_p2/bad_tag ,
 good_p3 ,bad_p3 ,good_p3/good_tag ,bad_p3/bad_tag ,good_p3/good_tag- bad_p3/bad_tag ,
 good_p4 ,bad_p4 ,good_p4/good_tag ,bad_p4/bad_tag ,good_p4/good_tag- bad_p4/bad_tag ,
 good_p5 ,bad_p5 ,good_p5/good_tag ,bad_p5/bad_tag ,good_p5/good_tag- bad_p5/bad_tag ,
 good_p6 ,bad_p6 ,good_p6/good_tag ,bad_p6/bad_tag ,good_p6/good_tag- bad_p6/bad_tag ,
 good_p7 ,bad_p7 ,good_p7/good_tag ,bad_p7/bad_tag ,good_p7/good_tag- bad_p7/bad_tag ,
 good_p8 ,bad_p8 ,good_p8/good_tag ,bad_p8/bad_tag ,good_p8/good_tag- bad_p8/bad_tag ,
 good_p9 ,bad_p9 ,good_p9/good_tag ,bad_p9/bad_tag ,good_p9/good_tag- bad_p9/bad_tag
 from
(
SELECT

sum( case when label=1 and p10<1 then 1 else 0  end ) as  good_p1 ,
sum( case when label=0 and p10<1 then 1 else 0  end ) as  bad_p1 ,
sum( case when label=1 and p10<2 then 1 else 0  end ) as  good_p2 ,
sum( case when label=0 and p10<2 then 1 else 0  end ) as  bad_p2 ,
sum( case when label=1 and p10<3 then 1 else 0  end ) as  good_p3 ,
sum( case when label=0 and p10<3 then 1 else 0  end ) as  bad_p3 ,
sum( case when label=1 and p10<4 then 1 else 0  end ) as  good_p4 ,
sum( case when label=0 and p10<4 then 1 else 0  end ) as  bad_p4 ,
sum( case when label=1 and p10<5 then 1 else 0  end ) as  good_p5 ,
sum( case when label=0 and p10<5 then 1 else 0  end ) as  bad_p5 ,
sum( case when label=1 and p10<6 then 1 else 0  end ) as  good_p6 ,
sum( case when label=0 and p10<6 then 1 else 0  end ) as  bad_p6 ,
sum( case when label=1 and p10<7 then 1 else 0  end ) as  good_p7 ,
sum( case when label=0 and p10<7 then 1 else 0  end ) as  bad_p7 ,
sum( case when label=1 and p10<8 then 1 else 0  end ) as  good_p8 ,
sum( case when label=0 and p10<8 then 1 else 0  end ) as  bad_p8 ,
sum( case when label=1 and p10<9 then 1 else 0  end ) as  good_p9 ,
sum( case when label=0 and p10<9 then 1 else 0  end ) as  bad_p9
from
(
SELECT
cast(predict*10 as int ) as p10 ,label
from
t_zlj_rong360_rs_ks where ds='v2'
)t

)t1
join
(
SELECT
sum(case when label=1 then 1 else 0 end ) as good_tag,
sum(case when label=0 then 1 else 0 end ) as bad_tag
from t_zlj_rong360_rs_ks where ds='v2'
)t2
;

-- v1
2       108     0.02531645569620253     0.8244274809160306      -0.799111025219828
2       117     0.02531645569620253     0.8931297709923665      -0.86781331529616393123
                0.0379746835443038      0.9389312977099237      -0.9009566141656199
3       125      0.0379746835443038      0.9541984732824428      -0.916223789738139
5        127      0.06329113924050633     0.9694656488549618      -0.9061745096144554
7       129      0.08860759493670886     0.9847328244274809      -0.896125229490772
11       129      0.13924050632911392     0.9847328244274809      -0.845492318098367
19      129      0.24050632911392406     0.9847328244274809      -0.7442264953135569
32      131      0.4050632911392405      1.0     -0.5949367088607596

-- v2
SELECT
 good_p1 ,bad_p1 ,good_p1/good_tag ,bad_p1/bad_tag ,good_p1/good_tag- bad_p1/bad_tag ,
 good_p2 ,bad_p2 ,good_p2/good_tag ,bad_p2/bad_tag ,good_p2/good_tag- bad_p2/bad_tag ,
 good_p3 ,bad_p3 ,good_p3/good_tag ,bad_p3/bad_tag ,good_p3/good_tag- bad_p3/bad_tag ,
 good_p4 ,bad_p4 ,good_p4/good_tag ,bad_p4/bad_tag ,good_p4/good_tag- bad_p4/bad_tag ,
 good_p5 ,bad_p5 ,good_p5/good_tag ,bad_p5/bad_tag ,good_p5/good_tag- bad_p5/bad_tag ,
 good_p6 ,bad_p6 ,good_p6/good_tag ,bad_p6/bad_tag ,good_p6/good_tag- bad_p6/bad_tag ,
 good_p7 ,bad_p7 ,good_p7/good_tag ,bad_p7/bad_tag ,good_p7/good_tag- bad_p7/bad_tag ,
 good_p8 ,bad_p8 ,good_p8/good_tag ,bad_p8/bad_tag ,good_p8/good_tag- bad_p8/bad_tag ,
 good_p9 ,bad_p9 ,good_p9/good_tag ,bad_p9/bad_tag ,good_p9/good_tag- bad_p9/bad_tag
 from
(
SELECT

sum( case when label=1 and p10<1 then 1 else 0  end ) as  good_p1 ,
sum( case when label=0 and p10<1 then 1 else 0  end ) as  bad_p1 ,
sum( case when label=1 and p10<2 then 1 else 0  end ) as  good_p2 ,
sum( case when label=0 and p10<2 then 1 else 0  end ) as  bad_p2 ,
sum( case when label=1 and p10<3 then 1 else 0  end ) as  good_p3 ,
sum( case when label=0 and p10<3 then 1 else 0  end ) as  bad_p3 ,
sum( case when label=1 and p10<4 then 1 else 0  end ) as  good_p4 ,
sum( case when label=0 and p10<4 then 1 else 0  end ) as  bad_p4 ,
sum( case when label=1 and p10<5 then 1 else 0  end ) as  good_p5 ,
sum( case when label=0 and p10<5 then 1 else 0  end ) as  bad_p5 ,
sum( case when label=1 and p10<6 then 1 else 0  end ) as  good_p6 ,
sum( case when label=0 and p10<6 then 1 else 0  end ) as  bad_p6 ,
sum( case when label=1 and p10<7 then 1 else 0  end ) as  good_p7 ,
sum( case when label=0 and p10<7 then 1 else 0  end ) as  bad_p7 ,
sum( case when label=1 and p10<8 then 1 else 0  end ) as  good_p8 ,
sum( case when label=0 and p10<8 then 1 else 0  end ) as  bad_p8 ,
sum( case when label=1 and p10<9 then 1 else 0  end ) as  good_p9 ,
sum( case when label=0 and p10<9 then 1 else 0  end ) as  bad_p9
from
(
SELECT
cast(predict*10 as int ) as p10 ,label
from
t_zlj_rong360_rs_ks where ds='v3'
)t

)t1
join
(
SELECT
sum(case when label=1 then 1 else 0 end ) as good_tag,
sum(case when label=0 then 1 else 0 end ) as bad_tag
from t_zlj_rong360_rs_ks where ds='v3'
)t2
;
测试结果  正 43/68
1084/1109=0.97

22	1014	0.328358209	0.973128599	-0.64477039
23	1035	0.343283582	0.99328215	-0.649998568
23	1039	0.343283582	0.997120921	-0.653837339
24	1039	0.358208955	0.997120921	-0.638911966
25	1042	0.373134328	1	-0.626865672
27	1042	0.402985075	1	-0.597014925
31	1042	0.462686567	1	-0.537313433
43	1042	0.641791045	1	-0.358208955
67	1042	1	1	0


SELECT  p10,count(1)
from
(
SELECT
cast(predict*100 as int ) as p10 ,label
from
t_zlj_rong360_rs_ks)t
group by p10

