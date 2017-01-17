item 评论表

t_base_weibo_text -> text,mid,user_id


create table if not exists wlcredit.t_lel_weibo_credit_feed as
select
transform(mid,user_id,text)
using 'python lel_udf.py'
as (mid,user_id,text,flag_1, kw_1, flag_2, kw_2,fraud_score,keywords)
from
(select
mid,user_id,text
from
wlbase_dev.t_base_weibo_text where ds=20161126
) t where cast(t.fraud_score as int) >=2



select count(1) from
(select user_id from
    t_lel_weibo_credit_feed
        where cast(fraud_score as int) >= 2 group by user_id) t;