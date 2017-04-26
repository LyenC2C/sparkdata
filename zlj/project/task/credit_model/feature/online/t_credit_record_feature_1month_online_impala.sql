drop table wl_feature.t_credit_record_feature_1month_online_impala;
create table wl_feature.t_credit_record_feature_1month_online_impala as
SELECT
tel_index,
round(sum(price),2)   month1_price_sum,
round(count(1) ,2)    month1_buy_count,
round(avg(price),2)   month1_price_avg,
round(max(price),2)   month1_price_max,
round(min(price),2)   month1_price_min,
round(stddev(price),2)   month1_price_std,
round(max(price)-min(price),2)      month1_price_cross ,
round(sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END),2)     as        month1_annoy_num,
round(sum(CASE WHEN annoy = '1'       THEN 1        ELSE 0 END)/COUNT(1),2)  as  month1_annoy_ratio,
round(sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END),2)        as          month1_b_bc_type_num,
round(sum(CASE WHEN bc_type = 'B'       THEN 1         ELSE 0 END)/COUNT(1),2)       as     month1_b_bc_type_num_ratio,
round(sum(CASE WHEN bc_type = 'B'       THEN price         ELSE 0 END)/sum(price),2)    as  month1_b_bc_price_ratio,
round(sum(CASE WHEN CAST (brand_id as bigint )>5       THEN price         ELSE 0 END)/sum(price),2)   as  month1_brand_effec_price_ratio,
round(sum(CASE WHEN CAST (brand_id as bigint )>5     THEN 1        ELSE 0 END)/COUNT(1),2)     as        month1_brand_effec_num_ratio,
round(sum(case when price <=50 then 1 else 0 end)/count(*),2)         as month1_b50_num_ratio,
round(sum(case when price <=50 then price else 0 end)/sum(price ),2)  as month1_b50_ratio,
round(sum(case when price <=30 then 1 else 0 end)/count(*),2)         as month1_b30_num_ratio,
round(sum(case when price <=30 then price else 0 end)/sum(price ),2)  as month1_b30_ratio,
round(sum(case when price <=10 then 1 else 0 end)/count(*),2)         as month1_b10_num_ratio,
round(sum(case when price <=10 then price else 0 end)/sum(price ),2)  as month1_b10_ratio,
round(sum(case when price <=5 then 1 else 0 end)/count(*),2)         as   month1_b5_num_ratio,
round(sum(case when price <=5 then price else 0 end)/sum(price ),2)  as   month1_b5_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=10 then 1 else 0 end)/count(*),2) as  month1_b50_10_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=9  then 1 else 0 end)/count(*),2) as  month1_b50_9_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=8  then 1 else 0 end)/count(*),2) as  month1_b50_8_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=7  then 1 else 0 end)/count(*),2) as  month1_b50_7_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=6  then 1 else 0 end)/count(*),2) as  month1_b50_6_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=5  then 1 else 0 end)/count(*),2) as  month1_b50_5_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=4  then 1 else 0 end)/count(*),2) as  month1_b50_4_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=3  then 1 else 0 end)/count(*),2) as  month1_b50_3_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=2  then 1 else 0 end)/count(*),2) as  month1_b50_2_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=1  then 1 else 0 end)/count(*),2) as  month1_b50_1_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=0  then 1 else 0 end)/count(*),2) as  month1_b50_0_num_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=10 then price else 0 end)/sum(price) ,2) as month1_b50_10_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=9  then price else 0 end)/sum(price),2) as month1_b50_9_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=8  then price else 0 end)/sum(price),2) as month1_b50_8_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=7  then price else 0 end)/sum(price),2) as month1_b50_7_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=6  then price else 0 end)/sum(price),2) as month1_b50_6_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=5  then price else 0 end)/sum(price),2) as month1_b50_5_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=4  then price else 0 end)/sum(price),2) as month1_b50_4_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=3  then price else 0 end)/sum(price),2) as month1_b50_3_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=2  then price else 0 end)/sum(price),2) as month1_b50_2_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=1  then price else 0 end)/sum(price),2) as month1_b50_1_ratio,
round(sum(case when price <=50 and CAST(price/5 as int )=0  then price else 0 end)/sum(price),2) as month1_b50_0_ratio,
round((sum(pow(2.8, datediff* (-0.005)))+20)/75,2)  as month1_active_score
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast (date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1) as string),'-','' ),1,6) <= ds
and
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1) as string),'-','' ),1,8) <dsn
group by tel_index
;