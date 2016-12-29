

-- 价格的加减乘除
drop table wlcredit.t_credit_record_cate1_feature_online ;
create table wlcredit.t_credit_record_cate1_feature_online as
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
tel_index,root_cat_id as cate_level1,
concat_ws(':', concat_ws('_',cast( root_cat_id as string),'sum_price_level1' ) ,cast(  round(sum(price),2) as string) ) price_sum,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'count_level1'     ) ,cast( round(count(1) ,2)  as string) ) buy_count,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'avg_price_level1' ) ,cast( round(avg(price),2) as string) ) price_avg,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'max_price_level1' ) ,cast( round(max(price),2) as string) ) price_max,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'min_price_level1' ) ,cast( round(min(price),2) as string) ) price_min,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'std_price_level1' ) ,cast( round(std(price),2) as string) ) price_std,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'median_price_level1') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'cross_price_level1' ) ,cast(round(max(price)-min(price),2) as string) ) price_cross ,

concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'price_025_level1') ,cast( round(percentile(cast(price as int),0.25),2) as string) ) price_025,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'price_010_level1') ,cast( round(percentile(cast(price as int),0.10),2) as string) ) price_010,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'price_075_level1') ,cast( round(percentile(cast(price as int),0.75),2) as string) ) price_075
wl_analysis.t_base_record_cate_simple

group by tel_index,root_cat_id
)t group by tel_index
;

-- SELECT
-- tel_index,root_cat_id as cate_level1,
-- concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'median_price_level1') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
-- concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'cross_price_level1' ) ,cast( round(max(price)-min(price),2) as string) ) price_max
-- from wlbase_dev.t_base_record_cate where tel_index is not null and tel_user_rn<4 and price<160000
-- and  root_cat_id is not null
-- group by tel_index,root_cat_id