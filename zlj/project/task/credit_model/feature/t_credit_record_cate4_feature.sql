
drop table wlcredit.t_credit_record_cate4_feature ;
create table wlcredit.t_credit_record_cate4_feature as
SELECT
tel_index , concat_ws(' ', collect_set(concat_ws(' ',
price_sum,
buy_count,
price_avg,
price_max,
price_min,
price_std,
price_median,
price_cross ,
price_025 ,
price_010 ,
price_075
))) as feature
from
(
SELECT

tel_index,cate_level4_id as cate_level4,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'sum_price_level4' ) ,cast( round(sum(price),2) as string) )  price_sum,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'count_level4'     ) ,cast( round(count(1) ,2)  as string) ) buy_count,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'avg_price_level4' ) ,cast( round(avg(price),2) as string) ) price_avg,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'max_price_level4' ) ,cast( round(max(price),2) as string) ) price_max,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'min_price_level4' ) ,cast( round(min(price),2) as string) ) price_min,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'std_price_level4' ) ,cast( round(std(price),2) as string) ) price_std ,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'median_price_level4') ,cast( round(percentile(cast(price as int),0.5),2) as string) ) price_median,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'cross_price_level4' ) ,cast( round(max(price)-min(price),2) as string) ) price_cross ,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'price_025_level4') ,cast( round(percentile(cast(price as int),0.25),2) as string) ) price_025,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'price_010_level4') ,cast( round(percentile(cast(price as int),0.10),2) as string) ) price_010,
concat_ws(':', concat_ws('_',cast( cate_level4_id as string) ,'price_075_level4') ,cast( round(percentile(cast(price as int),0.75),2) as string) ) price_075

from wlbase_dev.t_base_record_cate_simple_xianyu
 group by tel_index,cate_level4_id
)t group by tel_index
;
