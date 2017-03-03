drop table wl_feature.t_credit_record_feature_1month_online_hive;
create table wl_feature.t_credit_record_feature_1month_online_hive as
SELECT
tel_index,
round(percentile(cast(price as int),0.5),2)   month1_price_median,
round(percentile(cast(price as int),0.25),2)   month1_price_025,
round(percentile(cast(price as int),0.75),2)   month1_price_075,
round(percentile(cast(price as int),0.10),2)   month1_price_010,
round(COUNT(DISTINCT brand_id),2)   as  month1_brand_id_num,
round(COUNT(DISTINCT root_cat_id),2)  as month1_root_cat_id_num,
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1),'-','' ),1,6) <= ds
and
regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1),'-','' )<dsn
group by tel_index
;
