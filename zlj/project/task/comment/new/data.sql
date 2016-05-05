

create table t_zlj_feed_parse_alldata  as
select
item_id,user_id,content
from
t_base_ec_item_feed_dev
;



create table t_zlj_feed_parse_alldata_s as

select * from t_zlj_feed_parse_alldata where

length(content)>2 and content<>"ºÃÆÀ£¡"  and content not like "%Ä¬ÈÏ%";
