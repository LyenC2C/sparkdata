
select t2.*, case when t2.age<150  and t2.age >5 then t2.age  else t1.age end age
 from  qqage  t1  join
(select *,(2015-year(birthday)) age  from   t_base_q_user_dev )t2  on t1.qq=t2.uin
;



select * from t_base_q_user_dev where uin='469330328' ;

select *,   ROW_NUMBER() OVER(PARTITION BY item_id ORDER BY cast(ts AS BIGINT) DESC) AS rn

from t_base_q_user_dev_zlj ;



-- select * from t_base_ec_dim where cate_id=50015202;

create table t_zlj_ec_item_feed_count as
select item_id,count(1) as times, concat_ws('\001',collect_set(feed_id)) as feed_ids  from t_base_ec_item_feed_dev where ds>20131002 and ds<20151103
 group by item_id;