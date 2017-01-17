-- 价格的加减乘除
drop table wlcredit.t_credit_record_cate2_feature_months ;
create table wlcredit.t_credit_record_cate2_feature_months as
SELECT
tel_index , concat_ws(' ', collect_set(concat_ws(' ',
price_sum,
buy_count,
price_avg,
price_max,
price_min,
price_std ,
price_median,
price_cross ,
price_025 ,
price_010 ,
price_075
))) as feature
from
(
SELECT
tel_index,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'sum_price_level2' ) ,cast(  round(sum(price),2) as string) ) price_sum,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'count_level2'     ) ,cast( round(count(1) ,2)  as string) ) buy_count,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'avg_price_level2' ) ,cast( round(avg(price),2) as string) ) price_avg,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'max_price_level2' ) ,cast( round(max(price),2) as string) ) price_max,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'min_price_level2' ) ,cast( round(min(price),2) as string) ) price_min,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'std_price_level2' ) ,cast( round(std(price),2) as string) ) price_std,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'median_price_level2') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'cross_price_level2' ) ,cast(round(max(price)-min(price),2) as string) ) price_cross ,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'price_025_level2') ,cast( round(percentile(cast(price as int),0.25),2) as string) ) price_025,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'price_010_level2') ,cast( round(percentile(cast(price as int),0.10),2) as string) ) price_010,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string), month,'price_075_level2') ,cast( round(percentile(cast(price as int),0.75),2) as string) ) price_075
from 
(
select  tel_index,cate_level2_id,price,
case when regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1),'-','' )<dsn then "1month"
when regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*3),'-','' ) <= dsn then "3month"
when regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6),'-','' ) <= dsn then "6month"
when regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*12),'-','' ) <= dsn then "12month"
else "allmonth" 
end as month
from
wl_analysis.t_base_record_cate_simple_xianyu
where cate_level2_id is not null
)tt
group by tel_index,cate_level2_id,month
)t
group by tel_index
;

-- SELECT
-- tel_index,cate_level2_id as cate_level2,
-- concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'median_price_level2') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
-- concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'cross_price_level2' ) ,cast( round(max(price)-min(price),2) as string) ) price_max
-- from wlbase_dev.t_base_record_cate where tel_index is not null and tel_user_rn<4 and price<160000
-- and  cate_level2_id is not null
-- group by tel_index,cate_level2_id