drop table wl_feature.t_credit_record_feature_online_hive ;
create table wl_feature.t_credit_record_feature_online_hive as
SELECT
tel_index,
round(percentile(cast(price as int),0.5),2)   monthall_price_median,
round(percentile(cast(price as int),0.25),2)   monthall_price_025,
round(percentile(cast(price as int),0.75),2)   monthall_price_075,
round(percentile(cast(price as int),0.10),2)   monthall_price_010,
round(COUNT(DISTINCT brand_id),2)   as  monthall_brand_id_num,
round(COUNT(DISTINCT root_cat_id),2)  as monthall_root_cat_id_num,
from
wl_analysis.t_base_record_cate_simple_ds
group by tel_index
;

-- SELECT
-- tel_index,root_cat_id as cate_level1,
-- concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'median_price_level1') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
-- concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'cross_price_level1' ) ,cast( round(max(price)-min(price),2) as string) ) price_max
-- from wlbase_dev.t_base_record_cate where tel_index is not null and tel_user_rn<4 and price<160000
-- and  root_cat_id is not null
-- group by tel_index,root_cat_id