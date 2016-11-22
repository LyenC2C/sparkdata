-- rong360
-- Drop table t_zlj_tmp_rong360_1w_data ;
-- create table t_zlj_tmp_rong360_1w_data as
-- SELECT   /*+ mapjoin(t1)*/
-- -- t1.tel,
-- class ,
-- label,
-- gender,
-- age,
-- t2.*
-- from
-- (
-- SELECT  tel,'8000_c' as class , id1 as label,id3 as gender,id4 as age
--    from
--    wlfinance.t_zlj_base_match where ds='ygz_part'
--    union all
--     SELECT tel,'2000_c' as class,'' as label ,id1 as gender,id2 as age
--    from
--    wlfinance.t_zlj_base_match where ds='rong360_test_1111'
--    union all
--     SELECT  tel,'data_2k' as class , id1 as label,id3 as gender,id4 as age
--    from
--    wlfinance.t_zlj_base_match where ds='ygz_part'
-- )t1 join t_base_yhhx_model_tel t2 on  t1.tel=t2.tel  ;




Drop table wlservice.t_zlj_tmp_rong360_1w_record;
create table   wlservice.t_zlj_tmp_rong360_1w_record  as select * from t_zlj_tmp_rong360_1w_record ;
-- 数据
Drop table wlservice.t_zlj_tmp_rong360_1w_record;
create table wlservice.t_zlj_tmp_rong360_1w_record as
SELECT
 t2.*,
cate_level2_id ,
cate_level3_id ,
cate_level4_id ,
cate_level2_name ,
cate_level3_name ,
cate_level4_name
from
wlbase_dev.t_base_ec_dim t1 join
(
SELECT  /*+ mapjoin(t3)*/
tel, class,label ,gender,age,t4.*
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
t_base_ec_record_dev_new_telindex_fix t4 on t3.tel_index=t4.tel_index

)t2 on t1.cate_id=t2.cat_id ;






SELECT class,COUNT(1) from t_zlj_tmp_rong360_1w_record group by  class


-- 提取特征
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



-- select
-- from t_zlj_tmp_rong360_1w_record group by
--
-- -- check
-- 18782413399


-- 三级类目特征
create table wlservice.t_zlj_tmp_rong360_1w_record_level3_feature as
SELECT
tel , concat_ws(' ', collect_set(concat_ws(' ',
price_sum_rn,
buy_count_rn,
price_avg_rn,
price_max_rn,
price_min_rn,
price_std_rn)))
from
(
SELECT
tel ,
concat_ws(':', concat_ws('_' , cast(rn as string),'1')  , price_sum) price_sum_rn ,
concat_ws(':', concat_ws('_' , cast(rn as string),'2')  , buy_count) buy_count_rn ,
concat_ws(':', concat_ws('_' , cast(rn as string),'3')  , price_avg) price_avg_rn ,
concat_ws(':', concat_ws('_' , cast(rn as string),'4')  , price_max) price_max_rn ,
concat_ws(':', concat_ws('_' , cast(rn as string),'5')  , price_min) price_min_rn ,
concat_ws(':', concat_ws('_' , cast(rn as string),'6')  , price_std) price_std_rn
from
(
SELECT
tel,cate_level3,
cast( round(sum(price),2) as string) price_sum,
cast( round(count(1) ,2)  as string) buy_count,
cast( round(avg(price),2) as string) price_avg,
cast( round(max(price),2) as string) price_max,
cast( round(min(price),2) as string) price_min ,
cast( round(std(price),2) as string) price_std
from
(
SELECT
* ,
COALESCE(cate_level3_id,cate_level2_id) as cate_level3
from wlservice.t_zlj_tmp_rong360_1w_record where tel is not null
)t group by tel,cate_level3
)t1 join t_base_ec_dim t2 on t1.cate_level3=t2.cate_id
)t group by tel
;
