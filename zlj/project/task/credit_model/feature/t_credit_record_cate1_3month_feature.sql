--3个月用户购买记录分析

drop table wlcredit.t_credit_record_cate1_3month_feature ;
create table wlcredit.t_credit_record_cate1_3month_feature as
SELECT
tel_index , concat_ws(' ', collect_set(concat_ws(' ',
price_sum,
buy_count,
price_avg,
price_max,
price_min,
price_std ,
price_median,
price_cross
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
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'median_price_level1') ,cast( round(percentile(cast(price as int),0.5),2)
as string) ) price_median,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'cross_price_level1' ) ,cast( round(max(price)-min(price),2) as string) ) price_cross
from wlbase_dev.t_base_record_cate_simple
 where
 regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*3),'-','' )>dsn

group by tel_index,root_cat_id
)t group by tel_index
;