drop table wlservice.ppzs_brandid_6comment;
create table wlservice.ppzs_brandid_6comment as
select brand_id,rate_type,content,rn from
(
select brand_id,rate_type,content,
ROW_NUMBER() OVER (PARTITION BY brand_id,rate_type ORDER BY num DESC) as rn
from
(
select
case when t1.feed_id is null then t2.brand_id else t1.brand_id end as brand_id,
case when t1.feed_id is null then t2.content else t1.content end as content,
case when t1.feed_id is null then t2.rate_type else t1.rate_type end as rate_type,
rand() as num
from
(select * from wlservice.ppzs_brandid_feed
where ds = '${hiveconf:yes_day}' and content <> '评价方未及时做出评价,系统默认好评\!' and content <> '好评！' and rate_type = '1')t1
full JOIN
(select * from wlservice.ppzs_brandid_feed
where ds = '${hiveconf:yes_day}' and content <> '评价方未及时做出评价,系统默认好评\!' and content <> '好评！' and rate_type = '-1')t2
ON
t1.feed_id = t2.feed_id
)t
)tt
where rn < 7;

