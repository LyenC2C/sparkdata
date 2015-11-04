


select  user_id ,week ,count(1) as times
 from
(
select user_id, month()weekofyear(f_date)  as week
from t_base_ec_item_feed_dev
where ds>20150101
)