create table t_wrt_zhiniaoku_record as
select * from wlbase_dev.t_base_ec_record_dev_new where ds = 'true1'
and (cat_id = 121954006 or cat_id = 122326002 or cat_id = 50012481);


--create table t_wrt_znk_record like wlbase_dev.t_base_ec_record_dev_new;

CREATE EXTERNAL TABLE  if not exists t_wrt_znk_record (
item_id String COMMENT '商品id',
feed_id String COMMENT '评论id',
user_id String COMMENT '用户id',
dsn String COMMENT '评论日期'
)
COMMENT '纸尿裤项目专用评论表'
PARTITIONED BY  (ds STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n';

--表里的评论和采集的评论取并集
insert into table t_wrt_znk_record partition(ds = 20160826)
select
case when t2.item_id is null then t1.item_id else t2.item_id end,
case when t2.feed_id is null then t1.feed_id else t2.feed_id end,
case when t2.user_id is null then t1.user_id else t2.user_id end,
case when t2.dsns is null then t1.dsn else t2.dsns end
from
t_wrt_zhiniaoku_record t1
full join
t_zlj_tmp_record t2 --新采集的评论数据
on
t1.feed_id = t2.feed_id;

--
insert into table t_wrt_znk_record partition(ds = 20160829)
select
t1.item_id,
t1.feed_id,
t1.user_id,
t1.dsn
from
(select * from t_wrt_znk_record where ds = 20160826)t1
JOIN
(select item_id from t_wrt_znk_iteminfo where ds = 20160829)t2
ON
t2.item_id = t1.item_id;

--去重

insert into table t_wrt_znk_record partition(ds = 20160901)
select
max(item_id),
feed_id,
max(user_id),
max(dsn)
from
t_wrt_znk_record
where ds = 20160829
group by feed_id
;

--先将mark表中的mark替换成userid存到tt1表中，然后再与上次的评论表取一个并集,其中上次评论表的时间都往前推13天
insert overwrite table t_wrt_znk_record partition(ds = 20160919)
select
case when tt2.item_id is null then tt1.item_id else tt2.item_id end,
case when tt2.feed_id is null then tt1.feed_id else tt2.feed_id end,
case when tt2.user_id is null then tt1.user_id else tt2.user_id end,
case when tt2.dsn is null then tt1.dsn
else regexp_replace(date_sub(from_unixtime(unix_timestamp(tt2.dsn,'yyyyMMdd'),'yyyy-MM-dd'),13),'-','')
end
from
(
select
t1.item_id,
t1.feed_id,
t2.id1 as user_id,
t1.dsn
from
t_wrt_znk_feedmark t1
join
(select id1,uid from wlbase_dev.t_base_uid_tmp where ds = 'uid_mark') t2
ON
t1.usermark = t2.uid
)tt1
full JOIN
(select * from t_wrt_znk_record where ds = 20160901)tt2
ON
tt1.feed_id = tt2.feed_id;


