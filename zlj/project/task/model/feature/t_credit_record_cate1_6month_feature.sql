

--3个月用户购买记录分析

drop table wlcredit.t_credit_record_cate1_6month_feature ;
create table wlcredit.t_credit_record_cate1_6month_feature as
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
tel_index,root_cat_id as cate_level1,
concat_ws(':', concat_ws('_',cast( root_cat_id as string),'sum_price' ) ,cast(  round(sum(price),2) as string) )  price_sum,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'count'     ) ,cast( round(count(1) ,2)  as string) ) buy_count,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'avg_price' ) ,cast( round(avg(price),2) as string) ) price_avg,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'max_price' ) ,cast( round(max(price),2) as string) ) price_max,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'min_price' ) ,cast( round(min(price),2) as string) ) price_min,
concat_ws(':', concat_ws('_',cast( root_cat_id as string) ,'price_std' ) ,cast( round(std(price),2) as string) ) price_std
from wlbase_dev.t_base_record_cate where tel_index is not null and tel_user_rn<4 and price<160000

and regexp_replace(date_sub(from_unixtime( unix_timestamp() ,'yyyy-MM-dd'),30*6),'-','' )>dsn
and ds='true'
group by tel_index,root_cat_id
)t group by tel_index
;
