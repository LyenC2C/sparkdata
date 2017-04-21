



-- 价格的加减乘除
drop table wl_feature.t_credit_record_feature_6month_online ;
create table wl_feature.t_credit_record_feature_6month_online as
    SELECT
    tel_index,
    max(user_id) user_id ,
    round(sum(price),2)   month6_price_sum,
    round(count(1) ,2)    month6_buy_count,
    round(avg(price),2)   month6_price_avg,
    round(max(price),2)   month6_price_max,
    round(min(price),2)   month6_price_min,
    round(std(price),2)   month6_price_std,
    round(percentile(cast(price as int),0.5),2)   month6_price_median,
    round(max(price)-min(price),2)      month6_price_cross ,
    round(percentile(cast(price as int),0.25),2)   month6_price_025,
    round(percentile(cast(price as int),0.75),2)   month6_price_075,
    round(percentile(cast(price as int),0.10),2)   month6_price_010,

round(sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END),2)     as        month6_annoy_num,
round(sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)/COUNT(1),2)  as  month6_annoy_ratio,
round(COUNT(DISTINCT brand_id),2)   as  month6_brand_id_num,
round(COUNT(DISTINCT root_cat_id),2)  as month6_root_cat_id_num,
round(sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END),2)        as          month6_b_bc_type_num,
round(sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END)/COUNT(1),2)       as     month6_b_bc_type_num_ratio,
round(sum(CASE WHEN bc_type = 'B'       THEN price         ELSE 0 END)/sum(price),2)    as  month6_b_bc_price_ratio,
round(sum(CASE WHEN CAST (brand_id as bigint )>5       THEN price         ELSE 0 END)/sum(price),2)   as  month6_brand_effec_price_ratio,
round(sum(CASE WHEN CAST (brand_id as bigint )>5     THEN 1        ELSE 0 END)/COUNT(1),2)     as        month6_brand_effec_num_ratio,
round(sum(case when price <=50 then 1 else 0 end)/count(*),2)         as month6_b50_num_ratio,
round(sum(case when price <=50 then price else 0 end)/sum(price ),2)  as month6_b50_ratio,
round(sum(case when price <=30 then 1 else 0 end)/count(*),2)         as month6_b30_num_ratio,
round(sum(case when price <=30 then price else 0 end)/sum(price ),2)  as month6_b30_ratio,
round(sum(case when price <=10 then 1 else 0 end)/count(*),2)         as month6_b10_num_ratio,
round(sum(case when price <=10 then price else 0 end)/sum(price ),2)  as month6_b10_ratio,
round(sum(case when price <=5 then 1 else 0 end)/count(*),2)         as   month6_b5_num_ratio,
round(sum(case when price <=5 then price else 0 end)/sum(price ),2)  as   month6_b5_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==10 then 1 else 0 end)/count(*),2) as  month6_b50_10_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==9  then 1 else 0 end)/count(*),2) as  month6_b50_9_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==8  then 1 else 0 end)/count(*),2) as  month6_b50_8_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==7  then 1 else 0 end)/count(*),2) as  month6_b50_7_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==6  then 1 else 0 end)/count(*),2) as  month6_b50_6_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==5  then 1 else 0 end)/count(*),2) as  month6_b50_5_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==4  then 1 else 0 end)/count(*),2) as  month6_b50_4_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==3  then 1 else 0 end)/count(*),2) as  month6_b50_3_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==2  then 1 else 0 end)/count(*),2) as  month6_b50_2_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==1  then 1 else 0 end)/count(*),2) as  month6_b50_1_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==0  then 1 else 0 end)/count(*),2) as  month6_b50_0_num_ratio,

round(sum(case when price <=50 and CAST(price/5 as int )==10 then price else 0 end)/sum(price) ,2) as month6_b50_10_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==9  then price else 0 end)/sum(price),2) as month6_b50_9_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==8  then price else 0 end)/sum(price),2) as month6_b50_8_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==7  then price else 0 end)/sum(price),2) as month6_b50_7_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==6  then price else 0 end)/sum(price),2) as month6_b50_6_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==5  then price else 0 end)/sum(price),2) as month6_b50_5_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==4  then price else 0 end)/sum(price),2) as month6_b50_4_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==3  then price else 0 end)/sum(price),2) as month6_b50_3_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==2  then price else 0 end)/sum(price),2) as month6_b50_2_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==1  then price else 0 end)/sum(price),2) as month6_b50_1_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )==0  then price else 0 end)/sum(price),2) as month6_b50_0_ratio,
round((sum(pow(2.8, datediff* (-0.005)))+20)/75,2)  as month6_active_score
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6),'-','' ),1,6) <= ds
and
regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6),'-','' )<dsn
group by tel_index
;

-- SELECT
-- tel_index,root_cat_id as cate_level1,
-- concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'median_price_level1') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
-- concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'cross_price_level1' ) ,cast( round(max(price)-min(price),2) as string) ) price_max
-- from wlbase_dev.t_base_record_cate where tel_index is not null and tel_user_rn<4 and price<160000
-- and  root_cat_id is not null
-- group by tel_index,root_cat_id