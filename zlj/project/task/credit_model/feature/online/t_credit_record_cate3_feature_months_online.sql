-- 价格的加减乘除
drop table wl_feature.t_credit_record_cate3_feature_months ;
create table wl_feature.t_credit_record_cate3_feature_months as
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
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'sum_price_level3' ) ,cast(  round(sum(price),2) as string) ) price_sum,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'count_level3'     ) ,cast( round(count(1) ,2)  as string) ) buy_count,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'avg_price_level3' ) ,cast( round(avg(price),2) as string) ) price_avg,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'max_price_level3' ) ,cast( round(max(price),2) as string) ) price_max,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'min_price_level3' ) ,cast( round(min(price),2) as string) ) price_min,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'std_price_level3' ) ,cast( round(std(price),2) as string) ) price_std,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'median_price_level3') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'cross_price_level3' ) ,cast(round(max(price)-min(price),2) as string) ) price_cross ,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'price_025_level3') ,cast( round(percentile(cast(price as int),0.25),2) as string) ) price_025,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'price_010_level3') ,cast( round(percentile(cast(price as int),0.10),2) as string) ) price_010,
concat_ws(':', concat_ws('_',cast( cate_level3_id as string), month,'price_075_level3') ,cast( round(percentile(cast(price as int),0.75),2) as string) ) price_075
from
(
select  tel_index,cate_level3_id,price, "1month" as month
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast(date_sub(from_unixtime(unix_timestamp() ,'yyyy-MM-dd'),30*1)as string),'-','' ),1,6) <= ds
AND
regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*1),'-','' )<dsn
AND
cate_level3_id is not null
AND
root_cat_id in
(16,50002766,50008090,1625,50008165,30,50013864,1801,50018004,50004958,50010788,50025705,25,50016422,26,50006843,50014812,50007216,122852001,50016348,33,122952001,122928002,50006842,50016349,50010404,21,29,50013886,50010728)
UNION  ALL
select  tel_index,cate_level3_id,price, "3month" as month
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast(date_sub(from_unixtime(unix_timestamp() ,'yyyy-MM-dd'),30*3)as string),'-','' ),1,6) <= ds
and
regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*3),'-','' )<dsn
AND
cate_level3_id is not null
AND
root_cat_id in
(16,50002766,50008090,1625,50008165,30,50013864,1801,50018004,50004958,50010788,50025705,25,50016422,26,50006843,50014812,50007216,122852001,50016348,33,122952001,122928002,50006842,50016349,50010404,21,29,50013886,50010728)
UNION  ALL
select  tel_index,cate_level3_id,price, "6month" as month
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6)as string),'-','' ),1,6) <= ds
AND
regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6),'-','' )<dsn
AND
cate_level3_id is not null
AND
root_cat_id in
(16,50002766,50008090,1625,50008165,30,50013864,1801,50018004,50004958,50010788,50025705,25,50016422,26,50006843,50014812,50007216,122852001,50016348,33,122952001,122928002,50006842,50016349,50010404,21,29,50013886,50010728)
UNION  ALL
select  tel_index,cate_level3_id,price, "12month" as month
from
wl_analysis.t_base_record_cate_simple_ds
where
substr(regexp_replace(cast(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*12)as string),'-','' ),1,6) <= ds
and
regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*12),'-','' )<dsn
AND
cate_level3_id is not null
AND
root_cat_id in
(16,50002766,50008090,1625,50008165,30,50013864,1801,50018004,50004958,50010788,50025705,25,50016422,26,50006843,50014812,50007216,122852001,50016348,33,122952001,122928002,50006842,50016349,50010404,21,29,50013886,50010728)
UNION  ALL
select  tel_index,cate_level3_id,price, "allmonth" as month
from
wl_analysis.t_base_record_cate_simple_ds
WHERE
cate_level3_id is not null
AND
root_cat_id in
(16,50002766,50008090,1625,50008165,30,50013864,1801,50018004,50004958,50010788,50025705,25,50016422,26,50006843,50014812,50007216,122852001,50016348,33,122952001,122928002,50006842,50016349,50010404,21,29,50013886,50010728)
)tt
group by tel_index,cate_level3_id,month
)t
group by tel_index
;

-- SELECT
-- tel_index,cate_level3_id as cate_level3,
-- concat_ws(':', concat_ws('_',cast( cate_level3_id as string) ,'median_price_level3') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
-- concat_ws(':', concat_ws('_',cast( cate_level3_id as string) ,'cross_price_level3' ) ,cast( round(max(price)-min(price),2) as string) ) price_max
-- from wlbase_dev.t_base_record_cate where tel_index is not null and tel_user_rn<4 and price<160000
-- and  cate_level3_id is not null
-- group by tel_index,cate_level3_id