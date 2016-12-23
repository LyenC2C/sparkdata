item 评论表

t_base_ec_item_feed_dev_new -> content,fed_id,f_date,item_id,user_id


create table if not exists wlcredit.t_lel_credit_feed
select
transform(item_id,feed_id,f_date,content)
using 'python lel_udf.py'
as (item_id,feed_id,f_date,content,flag_1, kw_1, flag_2, kw_2,fraud_score,keywords)
from
(select
item_id,feed_id,f_date,content
from
wlbase_dev.t_base_ec_item_feed_dev_new
) t
