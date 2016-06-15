
-- 打分排序
create table t_zlj_shop_desc_score_rank as
SELECT
 t1.shop_id ,main_cat_name ,shop_name,desc_score, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY desc_score  ASC) AS rn
from
(
SELECT
shop_id ,shop_name ,5*desc_highgap/100.0 + 2*service_highgap/100.0 +1*wuliu_highgap/100.0  as desc_score
from
t_base_ec_shop_dev_new
where ds=20160613 and bc_type='B'
)t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id

;



-- 成长性
create table t_zlj_shop_growing_score_rank as
SELECT
 t1.shop_id ,main_cat_name ,shop_name,credit,total_month, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY growing_score  ASC) AS rn
from
(
SELECT
shop_id ,shop_name ,credit*100 as  credit ,(12*(2016-YEAR(starts))+MONTH(starts)) as total_month, credit*100.0/(12*(2016-YEAR(starts))+MONTH(starts)) as growing_score
from
t_base_ec_shop_dev_new
where ds=20160613 and bc_type='B'
)t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id ;



-- 销量排序
create table t_zlj_shop_sold_num_rank as
SELECT
 t1.shop_id ,main_cat_name  ,shop_name,tsnu, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY tsnu  ASC) AS rn
from
wlservice.t_zlj_shop_anay_statis_info
t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id ;

-- 销售额排序
create table t_zlj_shop_sold_price_rank as
SELECT
 t1.shop_id ,main_cat_name  ,shop_name ,tsmo, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY tsmo  ASC) AS rn
from
wlservice.t_zlj_shop_anay_statis_info
t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id ;


-- 库存排序
create table t_zlj_shop_quant_rank as
SELECT
 t1.shop_id ,main_cat_name  ,shop_name,trenu, ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY trenu  ASC) AS rn
from
wlservice.t_zlj_shop_anay_statis_info
t1 join t_base_shop_major t2 on t1.shop_id=t2.shop_id ;



create table t_zlj_shop_result_rank as

SELECT *,ROW_NUMBER() OVER (PARTITION BY main_cat_name ORDER BY sum_score DESC) AS rn
from
(
select t1.shop_id ,t1.main_cat_name,t1.shop_name,t1.rn as t1rn,t2.rn t2rn,t3.rn t3rn ,t4.rn t4rn,desc_score,tsnu,tsmo,trenu,

    log2(t1.rn)*5+log2(t2.rn)*5+log2(t3.rn)*5+log2(t4.rn)*2
   as sum_score
   from
t_zlj_shop_desc_score_rank t1 join
t_zlj_shop_sold_num_rank t2 on t1.shop_id=t2.shop_id
join t_zlj_shop_sold_price_rank t3 on t1.shop_id=t3.shop_id
join t_zlj_shop_quant_rank t4 on t1.shop_id=t4.shop_id
)t
;


-- limit 100;



SELECT  * FROM  t_zlj_shop_result_rank where  rn<50

小米店铺名  104736810


SELECT  * from t_base_ec_shop_dev_new where ds=20160613 and  shop_name like '%环宇乐器导购时尚女装%'
-- 62856546        521175107       环宇乐器导购时尚女装    孙绍雨  0       12      2010-09-21 15:50:04     C
-- 44      1844    98.24   2072872998      4.7     4.7     4.7 江苏徐州        1463393328      -0.29   -0.2    -0.1    0       -       -       20160613

SELECT  * from t_base_ec_shop_dev_new where ds=20160613 and  shop_name like '%嘉悦外贸专注性价比%'
-- 148953764       2807905403      嘉悦外贸专注性价比      嘉悦外贸专注性价比      0       6       2016-02-22 20:50:23     C
-- 148     315     98.28   10000000028079054034.7      4.8     4.6     江苏苏州        1464683988      17.97   22.36   -1.7    0       -       -       20160613


SELECT  * from t_base_ec_shop_dev_new where ds=20160613 and  shop_name like '%一起游数码专营店%'

-- 112182080       2151742315      一起游数码专营店        一起游数码专营店        99      12      2014-07-16 21:54:39     B
--  1398    4733    100.0   10000000021517423154.9      4.9     4.9     四川成都        1464362873      69.09   68.67   77.4    0       -       -       20160613

