create table t_wrt_children_keywords_record AS
select t2.* from
(select user_id from wlbase_dev.t_zlj_credit_children_feed_data group by user_id)t1
join
(select user_id,root_cat_id,root_cat_name,cat_id from wlbase_dev.t_base_ec_record_dev_new)t2
on
t1.user_id = t2.user_id