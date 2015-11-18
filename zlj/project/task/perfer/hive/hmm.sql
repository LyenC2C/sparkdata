


create table  t_zlj_userbuy_item_hmm

AS

select
user_id, concat_ws('_', collect_set(hmm))

from
(
select item_id,hmm from t_zlj_item_hmm
)t1
join

(
select item_id,user_id from t_base_ec_item_feed_dev

where ds>20150101
and  LENGTH (user_id)>0

)t2

on t1.item_id=t2.item_id
group by user_id
;