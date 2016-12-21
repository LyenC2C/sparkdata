
drop table wlcredit.t_credit_record_cate2_feature ;
create table wlcredit.t_credit_record_cate2_feature as
SELECT
tel_index , concat_ws(' ', collect_set(concat_ws(' ',
price_sum,
buy_count,
price_avg,
price_max,
price_min,
price_std
))) as feature
from
(
SELECT

tel_index,cate_level2_id as cate_level2,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string),'sum_price_level2' ) ,cast( round(sum(price),2) as string) )  price_sum,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'count_level2'     ) ,cast( round(count(1) ,2)  as string) ) buy_count,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'avg_price_level2' ) ,cast( round(avg(price),2) as string) ) price_avg,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'max_price_level2' ) ,cast( round(max(price),2) as string) ) price_max,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'min_price_level2' ) ,cast( round(min(price),2) as string) ) price_min,
concat_ws(':', concat_ws('_',cast( cate_level2_id as string) ,'std_price_level2' ) ,cast( round(std(price),2) as string) ) price_std

from wlbase_dev.t_base_record_cate_simple
 group by tel_index,cate_level2_id

)t group by tel_index
;
