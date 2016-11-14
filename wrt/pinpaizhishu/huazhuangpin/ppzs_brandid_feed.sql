create TABLE wlservice.ppzs_brandid_feed(
brand_id string,
feed_id string,
item_id string,
user_id string,
rate_type string,
content string
)
COMMENT '品牌指数项目品牌评论表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;

insert overwrite table wlservice.ppzs_brandid_feed partition(ds = '20161108')
select
t1.brand_id,
t2.feed_id ,
t2.item_id ,
t2.user_id ,
t2.rate_type ,
t2.content
from
(select brand_id,item_id from ppzs_itemid_info where ds = 20161108)t1
JOIN
(select feed_id,item_id,user_id,rate_type,content from wlbase_dev.t_base_ec_item_feed_dev_new where ds>20160808)t2
ON
t1.item_id = t2.item_id



--------------------------------------------------------------------------
create TABLE wlservice.ppzs_brandid_rate_count(
brand_id string,
good_count string,
mid_count string,
bad_count string
)
COMMENT '品牌指数项目品牌好中差评数量'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'   LINES TERMINATED BY '\n'
stored as textfile ;


insert overwrite table wlservice.ppzs_brandid_rate_count partition(ds = '20161108')
SELECT
case when t12.brand_id is null then t3.brand_id else t12.brand_id end as brand_id,
case when t12.good_count is null then 0 else t12.good_count end as good_count,
case when t12.mid_count is null then 0 else t12.mid_count end as mid_count,
case when t3.ct is null then 0 else t3.ct end as bad_count
from
(
select
case when t1.brand_id is null then t2.brand_id else t1.brand_id end as brand_id,
case when t1.ct is null then 0 else t1.ct end as good_count,
case when t2.ct is null then 0 else t2.ct end as mid_count from
(select brand_id,count(1) as ct from ppzs_brandid_feed
where ds = 20161108 and rate_type = '1' group by brand_id)t1
full JOIN
(select brand_id,count(1) as ct from ppzs_brandid_feed
where ds = 20161108 and rate_type = '0' group by brand_id)t2
ON
t1.brand_id = t2.brand_id
)t12
full join
(
select brand_id,count(1) as ct from ppzs_brandid_feed
where ds = 20161108 and rate_type = '-1' group by brand_id
)t3
ON
t12.brand_id = t3.brand_id;

--------------------------------------------------------------
hive -e "
select t1.brand_id,t1.weeksold,t2.good_count,t2.mid_count,t2.bad_count from
(select * from wlservice.ppzs_brandid_weeksold where ds = '20161106')t1
join
(select * from wlservice.ppzs_brandid_rate_count where ds = '20161108')t2
ON
t1.brand_id = t2.brand_id
" >> ppzs_brandid_weeksold_feedcount_test

