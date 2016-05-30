-- 产品 评价语料
-- 化妆品 酒类 面膜

create table t_zlj_feed_parse_data_0517 as
select
item_id,
user_id,
t1.feed_id,
content

from
(
SELECT

item_id,
user_id,
feed_id
from
t_base_ec_record_dev_new
where  root_cat_id in (
'50008141',
'50025705',
'1801' ,
'50016348'
)

)t1 join
 (

 select
  feed_id,content
 from
t_base_ec_item_feed_dev

 )t2 on t1.feed_id =t2.feed_id  ;



 